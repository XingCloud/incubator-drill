package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.engine.async.AbstractRelayRecordBatch;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.JoinPOP;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.util.hash.OffHeapIntIntOpenHashMap;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.ValueVector.Mutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:57 AM
 */
public class JoinBatch extends BaseRecordBatch {

  final static Logger logger = LoggerFactory.getLogger(JoinBatch.class);
  private static LeftKeyCacheManager leftKeyCacheManager = new LeftKeyCacheManager();
  private FragmentContext context;
  private JoinPOP config;
  private RecordBatch leftIncoming;
  private RecordBatch rightIncoming;
  private BatchSchema batchSchema;
  private BasicEvaluator leftEvaluator;
  private BasicEvaluator rightEvaluator;
  private Connector connector;
  private LeftCache leftCache;
  private RightCache rightCache;

  private boolean leftFinished = false;
  private boolean rightFinished = false;
  private boolean newSchema = true;


  public JoinBatch(FragmentContext context, JoinPOP config, RecordBatch leftIncoming, RecordBatch rightIncoming) {
    this.context = context;
    this.config = config;
    this.leftIncoming = leftIncoming;
    this.rightIncoming = rightIncoming;
    this.leftCache = new LeftCache();
    this.rightCache = new RightCache();

    switch (config.getType()) {
      case LEFT:
        connector = new LeftConnector();
        break;
      case RIGHT:
        connector = new RightConnector();
        break;
      case INNER:
        connector = new InnerConnector();
        break;
      case ANTI:
        connector = new AntiConnector();
        break;
    }

    setupEvals();
  }

  @Override
  public void setupEvals() {
    JoinCondition condition = config.getConditoin();
    EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
    leftEvaluator = evaluatorFactory.getBasicEvaluator(leftIncoming, condition.getLeft());
    rightEvaluator = evaluatorFactory.getBasicEvaluator(rightIncoming, condition.getRight());
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return batchSchema;
  }

  @Override
  public void kill() {
    releaseAssets();
    leftIncoming.kill();
    rightIncoming.kill();
  }

  @Override
  public IterOutcome next() {
    if (!leftFinished) {
      IterOutcome lo = leftIncoming.next();
      switch (lo) {
        case STOP:
          return lo;
        case OK_NEW_SCHEMA:
        case OK:
          cacheLeft();
          break;
        case NOT_YET:
          break;
        case NONE:
          leftFinished = true;
      }
    }

    if (!rightFinished) {
      IterOutcome ro = rightIncoming.next();
      switch (ro) {
        case STOP:
          return ro;
        case OK_NEW_SCHEMA:
        case OK:
          cacheRight();
          break;
        case NOT_YET:
          break;
        case NONE:
          rightFinished = true;
      }
    }
    if (leftFinished) {
      if (rightCache.size() == 0) {
        if (rightFinished) {
          clearCache();
          return IterOutcome.NONE;
        } else {
          return IterOutcome.NOT_YET;
        }
      }
      while (rightCache.size() != 0) {
        connector.setup();
        if (!connector.connect()) {
          connector.clear();
          continue;
        }
        connector.upstream();
        connector.clear();
        if (newSchema) {
          newSchema = false;
          setupSchema();
          return IterOutcome.OK_NEW_SCHEMA;
        } else {
          return IterOutcome.OK;
        }
      }
      if (rightFinished) {
        clearCache();
        return IterOutcome.NONE;
      }
      return IterOutcome.NOT_YET;
    }
    return IterOutcome.NOT_YET;
  }

  private void cacheLeft() {
    IntVector key = (IntVector) leftEvaluator.eval();
    leftCache.cache(TransferHelper.transferVectors(leftIncoming), key);
    leftCache.setSchema(leftIncoming.getSchema());
  }

  private void cacheRight() {
    IntVector key = (IntVector) rightEvaluator.eval();
    rightCache.cache(TransferHelper.transferVectors(rightIncoming), key, rightIncoming.getRecordCount());
    rightCache.setSchema(rightIncoming.getSchema());
  }

  public void setupSchema() {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : outputVectors) {
      schemaBuilder.addField(v.getField());
    }
    batchSchema = schemaBuilder.build();
  }

  public int encode(int batch, int row) {
    if (batch > 0x0000ffff || row > 0x0000ffff) {
      logger.error("Encode batch batch & row failed . batch : {} , row {} ", batch, row);
      throw new DrillRuntimeException("Encode batch & row failed .");
    }
    return (batch << 16) | row;
  }

  public void decode(int position, Pair<Integer, Integer> pair) {
    pair.setFirst(position >>> 16);
    pair.setSecond(position & 0x0000ffff);
  }


  @Override
  public void releaseAssets() {
    clearCache();
    for (ValueVector v : outputVectors) {
      v.close();
    }
  }

  private void clearCache() {
    leftCache.clear();
    rightCache.clear();
  }

  abstract class Connector {

    protected List<int[]> outRecords = Lists.newArrayList();
    OffHeapIntIntOpenHashMap leftValueMap;
    protected List<List<ValueVector>> leftIncomings;
    protected List<MaterializedField> leftFields;
    protected List<ValueVector> rightVectors;
    protected IntVector rightJoinKey;
    protected List<MaterializedField> rightFields;
    protected int rightValueCount;
    protected MaterializedField leftKey;
    protected MaterializedField rightKey;
    protected List<ValueVector> leftOutPut;
    protected List<ValueVector> rightOutPut;

    public void setup() {
      leftIncomings = (List<List<ValueVector>>) leftCache.getIncomings();
      leftFields = leftCache.getFields();
      Tuple<List<ValueVector>, IntVector, Integer> tuple = rightCache.removeFirst();
      rightVectors = tuple.getFirst();
      rightJoinKey = tuple.getSecond();
      rightValueCount = tuple.getThird();
      rightFields = rightCache.getFields();
      leftValueMap = leftCache.getValuesIndexMap();
      leftKey = leftCache.keyField;
      rightKey = rightCache.keyField;
    }

    public abstract boolean connect();

    public void upstream() {
      beforeCopy();
      copyLeft();
      copyRight();
      afterCopy();
    }

    public void beforeCopy() {
      outputVectors.clear();
      leftOutPut = Lists.newArrayList();
      rightOutPut = Lists.newArrayList();
    }

    public void afterCopy() {
      for (ValueVector v : leftOutPut) {
        v.getMutator().setValueCount(recordCount);
        outputVectors.add(v);
      }
      for (ValueVector v : rightOutPut) {
        v.getMutator().setValueCount(recordCount);
        outputVectors.add(v);
      }
      outRecords.clear();
    }

    public void copyLeft() {
      for (int fieldId = 0; fieldId < leftFields.size(); fieldId++) {
        MaterializedField f = leftFields.get(fieldId);
        if (f.equals(leftKey)) {
          continue;
        } else if (leftKey == null && f.getName().equals(rightKey.getName())) {
          continue;
        }
        ValueVector out = TypeHelper.getNewVector(getMaterializedField(f), context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 4);
        Mutator outMutator = out.getMutator();
        ValueVector.Accessor current = null;
        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          current = leftIncomings.get(indexes[0]).get(fieldId).getAccessor();
          outMutator.setObject(i, current.getObject(indexes[1]));
        }
        leftOutPut.add(out);
      }
    }

    public void copyRight() {
      for (int fieldId = 0; fieldId < rightFields.size(); fieldId++) {
        MaterializedField f = rightFields.get(fieldId);
        ValueVector out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 4);
        Mutator outMutator = out.getMutator();
        ValueVector.Accessor accessor = rightVectors.get(fieldId).getAccessor();
        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, accessor.getObject(indexes[2]));
        }
        rightOutPut.add(out);
      }
    }

    public abstract MaterializedField getMaterializedField(MaterializedField f);


    public void clear() {
      if (rightJoinKey != null) {
        rightJoinKey.close();
        rightJoinKey = null;
      }
      if (rightVectors != null) {
        for (ValueVector v : rightVectors) {
          v.close();
        }
        rightVectors.clear();
        rightVectors = null;
      }
    }
  }


  // inner join
  class InnerConnector extends Connector {

    @Override
    public boolean connect() {
      IntVector.Accessor accessor = rightJoinKey.getAccessor();
      Pair<Integer, Integer> pair = new Pair<>();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        int value = leftValueMap.get(accessor.get(i)) ;
        if (value != OffHeapIntIntOpenHashMap.EMPTY_VALUE) {
          decode(value, pair);
          outRecords.add(new int[]{pair.first, pair.second, i});
        }
      }
      return !outRecords.isEmpty();
    }

    @Override
    public void afterCopy() {
      super.afterCopy();
    }

    @Override
    public void beforeCopy() {
      super.beforeCopy();
      recordCount = outRecords.size();
    }

    public MaterializedField getMaterializedField(MaterializedField f) {
      return f;
    }
  }

  // right outer join
  class RightConnector extends Connector {

    BitVector rightMarkBits = new BitVector(null, getContext().getAllocator());
    List<Integer> misMatchIndex;

    @Override
    public void beforeCopy() {
      super.beforeCopy();
      getMisMatch();
    }

    @Override
    public void afterCopy() {
      copyMisMatch();
      super.afterCopy();
    }

    private void getMisMatch() {
      misMatchIndex = Lists.newArrayList();
      int misMatchCount = 0;
      BitVector.Accessor bitAccessor = rightMarkBits.getAccessor();
      for (int i = 0; i < bitAccessor.getValueCount(); i++) {
        if (bitAccessor.get(i) == 0) {
          misMatchIndex.add(i);
          misMatchCount++;
        }
      }
      recordCount = outRecords.size() + misMatchCount;
      rightMarkBits.close();
    }

    private void copyMisMatch() {
      for (int i = 0; i < leftOutPut.size(); i++) {
        ValueVector.Mutator out = leftOutPut.get(i).getMutator();
        for (int j = outRecords.size(); j < recordCount; j++) {
          out.setObject(j, null);
        }
      }
      for (int i = 0; i < rightOutPut.size(); i++) {
        ValueVector.Accessor in = rightVectors.get(i).getAccessor();
        ValueVector.Mutator out = rightOutPut.get(i).getMutator();
        int index = outRecords.size();
        for (int j : misMatchIndex) {
          out.setObject(index++, in.getObject(j));
        }
      }
    }

    @Override
    public boolean connect() {
      rightMarkBits.allocateNew(rightValueCount);
      BitVector.Mutator mutator = rightMarkBits.getMutator();
      mutator.setValueCount(rightValueCount);
      IntVector.Accessor accessor = rightJoinKey.getAccessor();
      Pair<Integer, Integer> pair = new Pair<>();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        int value = leftValueMap.get(accessor.get(i));
        if (value != OffHeapIntIntOpenHashMap.EMPTY_VALUE) {
          decode(value, pair);
          outRecords.add(new int[]{pair.first, pair.second, i});
          mutator.set(i, 1);
        }
      }
      return true;
    }

    public MaterializedField getMaterializedField(MaterializedField f) {
      return MaterializedField.create(new SchemaPath(f.getName(), ExpressionPosition.UNKNOWN)
        , Types.optional(f.getType().getMinorType()));
    }

    @Override
    public void clear() {
      super.clear();
      rightMarkBits.close();
    }
  }

  class LeftConnector extends Connector {

    @Override
    public void upstream() {

    }

    @Override
    public boolean connect() {
      return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public MaterializedField getMaterializedField(MaterializedField f) {
      return null;
    }
  }

  // anti join
  class AntiConnector extends Connector {

    private final static int INVALID_INDEX = -1;

    @Override
    public boolean connect() {
      IntVector.Accessor accessor = rightJoinKey.getAccessor();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        if (!leftValueMap.containsKey(accessor.get(i))) {
          outRecords.add(new int[]{INVALID_INDEX, INVALID_INDEX, i});
        }
      }
      return !outRecords.isEmpty();
    }

    @Override
    public void beforeCopy() {
      super.beforeCopy();
      recordCount = outRecords.size();
    }

    @Override
    public void copyLeft() {
      // Do nothing
      // no need to upstream left values
    }

    @Override
    public MaterializedField getMaterializedField(MaterializedField f) {
      return f;
    }
  }

  class Cache {
    Collection<List<ValueVector>> incomings;
    List<MaterializedField> fields;
    MaterializedField keyField;
    boolean isSet;

    Cache() {
      fields = Lists.newArrayList();
      isSet = false;
    }

    public List<MaterializedField> getFields() {
      return fields;
    }

    public Collection<List<ValueVector>> getIncomings() {
      return incomings;
    }

    public int size() {
      return incomings.size();
    }

    void setSchema(BatchSchema schema) {
      if (!isSet) {
        isSet = true;
        for (MaterializedField f : schema) {
          this.fields.add(f);
        }
        Collections.sort(this.fields, new MaterializedFieldComparator());
      }
    }

    public void clear() {
      for (List<ValueVector> vectors : incomings) {
        for (ValueVector v : vectors) {
          v.close();
        }
      }
    }
  }

  class LeftCache extends Cache {

    //  Empty map for default

    OffHeapIntIntOpenHashMap valuesIndexMap = null;
    AtomicInteger publicCacheStatus = null;
    int localCacheStatus = 0;
    boolean init = false ;

    LeftCache() {
      super();
      Pair<AtomicInteger, OffHeapIntIntOpenHashMap> pair = leftKeyCacheManager.getKeyMap(((AbstractRelayRecordBatch) leftIncoming).getIncoming(), context.getAllocator());
      publicCacheStatus = pair.first;
      valuesIndexMap = pair.second;
      incomings = Lists.newArrayList();
    }

    public void cache(List<ValueVector> incoming, IntVector joinKey) {

      Collections.sort(incoming, new VectorComparator());
      incomings.add(incoming);
      cacheJoinKey(joinKey);

    }


    private void cacheJoinKey(IntVector joinKey) {
      synchronized (valuesIndexMap) {
        if (publicCacheStatus.compareAndSet(localCacheStatus, localCacheStatus + 1)) {
          int index = incomings.size() - 1;
          IntVector.Accessor accessor = joinKey.getAccessor();
          for (int i = 0; i < accessor.getValueCount(); i++) {
            valuesIndexMap.put(accessor.get(i), encode(index, i));
          }
        }
        localCacheStatus++;
        keyField = joinKey.getField();
        joinKey.close();
      }
    }

    @Override
    public List<MaterializedField> getFields() {
      if (fields.size() == 0) {
        if (leftIncoming.getSchema() != null) {
          for (MaterializedField f : leftIncoming.getSchema()) {
            fields.add(f);
            Collections.sort(fields, new MaterializedFieldComparator());
          }
        }
      }
      return super.getFields();
    }

    OffHeapIntIntOpenHashMap getValuesIndexMap() {
      return valuesIndexMap;
    }

    @Override
    public void clear() {
      if (valuesIndexMap != null) {
        if (valuesIndexMap.release()) {
          leftKeyCacheManager.remove(((AbstractRelayRecordBatch) leftIncoming).getIncoming());
        }
      }
      super.clear();
    }
  }

  class RightCache extends Cache {
    Deque<IntVector> joinKeys;
    Deque<Integer> recordCounts;

    RightCache() {
      super();
      incomings = new LinkedList<>();
      joinKeys = new LinkedList<>();
      recordCounts = new LinkedList<>();
    }

    public void cache(List<ValueVector> incoming, IntVector joinKey, int recordCount) {
      Collections.sort(incoming, new VectorComparator());
      ((LinkedList<List<ValueVector>>) incomings).addLast(incoming);
      joinKeys.addLast(joinKey);
      keyField = joinKey.getField();
      recordCounts.addLast(recordCount);
    }

    public Tuple<List<ValueVector>, IntVector, Integer> removeFirst() {
      return new Tuple<>(((LinkedList<List<ValueVector>>) incomings).removeFirst(),
        joinKeys.removeFirst(), recordCounts.removeFirst());
    }

    @Override
    public void clear() {
      super.clear();
      for (ValueVector v : joinKeys) {
        v.close();
      }
    }
  }

  public static class Pair<First, Second> {
    First first;
    Second second;

    public Pair() {
    }

    Pair(First first, Second second) {
      this.first = first;
      this.second = second;
    }

    First getFirst() {
      return first;
    }

    void setFirst(First first) {
      this.first = first;
    }

    Second getSecond() {
      return second;
    }

    void setSecond(Second second) {
      this.second = second;
    }
  }

  public static class Tuple<First, Second, Third> {

    First first;
    Second second;
    Third third;

    Tuple(First first, Second second, Third third) {
      this.first = first;
      this.second = second;
      this.third = third;
    }

    First getFirst() {
      return first;
    }

    Second getSecond() {
      return second;
    }

    Third getThird() {
      return third;
    }
  }

  public static class LeftKeyCacheManager {

    final static Logger logger = org.slf4j.LoggerFactory.getLogger(LeftKeyCacheManager.class);

    Map<RecordBatch, Pair<AtomicInteger, OffHeapIntIntOpenHashMap>> keyCacheMap = new HashMap<>();

    public Pair<AtomicInteger, OffHeapIntIntOpenHashMap> getKeyMap(RecordBatch recordBatch, BufferAllocator allocator) {
      synchronized (this) {
        Pair<AtomicInteger, OffHeapIntIntOpenHashMap> pair = keyCacheMap.get(recordBatch);
        if (pair == null) {
          pair = new Pair<>(new AtomicInteger(0), new OffHeapIntIntOpenHashMap(allocator));
          keyCacheMap.put(recordBatch, pair);
        } else {
          logger.info("Cache hit . ");
          pair.second.retain();
        }
        return pair;
      }
    }

    public void remove(RecordBatch recordBatch) {
      synchronized (this) {
        keyCacheMap.remove(recordBatch);
        logger.info("Cached keymap size : {}", keyCacheMap.size());
      }
    }


  }

  public class VectorComparator implements Comparator<ValueVector> {
    @Override
    public int compare(ValueVector left, ValueVector right) {
      return left.getField().getName().compareTo(right.getField().getName());
    }
  }

  public class MaterializedFieldComparator implements Comparator<MaterializedField> {
    @Override
    public int compare(MaterializedField left, MaterializedField right) {
      return left.getName().compareTo(right.getName());
    }
  }

}
