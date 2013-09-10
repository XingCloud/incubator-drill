package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.JoinPOP;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.vector.ValueVector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:57 AM
 */
public class JoinBatch extends BaseRecordBatch {

  final static Logger logger = LoggerFactory.getLogger(JoinBatch.class);
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
      return IterOutcome.NOT_YET;
    }
    return IterOutcome.NOT_YET;
  }

  private void cacheLeft() {
    IntVector key =  (IntVector) leftEvaluator.eval() ;
    leftCache.cache(TransferHelper.transferVectors(leftIncoming),key );
    leftCache.setSchema(leftIncoming.getSchema());
  }

  private void cacheRight() {
    IntVector key = (IntVector) rightEvaluator.eval()  ;
    rightCache.cache(TransferHelper.transferVectors(rightIncoming), key , rightIncoming.getRecordCount());
    rightCache.setSchema(rightIncoming.getSchema());
  }

  public void setupSchema() {
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : outputVectors) {
      schemaBuilder.addField(v.getField());
    }
    batchSchema = schemaBuilder.build();
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

  public static ValueVector getVector(MaterializedField f, List<ValueVector> vectors) {
    for (ValueVector vector : vectors) {
      if (vector.getField().equals(f)) {
        return vector;
      }
    }
    return null;
  }


  abstract class Connector {

    protected List<int[]> outRecords = Lists.newArrayList();
    IntObjectOpenHashMap<int[]> leftValueMap;
    protected List<List<ValueVector>> leftIncomings;
    protected BatchSchema leftSchema;
    protected List<ValueVector> rightVectors;
    protected IntVector rightJoinKey;
    protected BatchSchema rightSchema;
    protected int rightValueCount;
    protected MaterializedField keyField;

    public abstract void upstream();

    public abstract boolean connect();

    public void setup() {
      leftIncomings = leftCache.getIncomings();
      leftSchema = leftCache.getSchema();
      RightCache.Tuple<List<ValueVector>, IntVector, Integer> tuple = rightCache.removeFirst();
      rightVectors = tuple.getFirst();
      rightJoinKey = tuple.getSecond();
      rightValueCount = tuple.getThird();
      rightSchema = rightCache.getSchema();
      leftValueMap = leftCache.getValuesIndexMap();
      keyField = leftCache.keyField;
    }

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
    public void upstream() {
      outputVectors.clear();
      recordCount = outRecords.size();
      ValueVector out;
      Mutator outMutator;
      for (MaterializedField f : leftSchema) {
        out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 8);
        outMutator = out.getMutator();
        ValueVector.Accessor current = null;
        int cursor = -1;
        for (int i = 0; i < recordCount; i++) {
          int[] indexes = outRecords.get(i);
          if (indexes[0] != cursor) {
            cursor = indexes[0];
            current = getVector(f, leftIncomings.get(indexes[0])).getAccessor();
          }
          outMutator.setObject(i, current.getObject(indexes[1]));
        }
        outMutator.setValueCount(recordCount);
        outputVectors.add(out);
      }
      for (MaterializedField f : rightSchema) {
        out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 8);
        outMutator = out.getMutator();
        ValueVector.Accessor accessor = getVector(f, rightVectors).getAccessor();
        for (int i = 0; i < recordCount; i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, accessor.getObject(indexes[2]));
        }
        outMutator.setValueCount(recordCount);
        outputVectors.add(out);
      }
      outRecords.clear();
    }

    @Override
    public boolean connect() {
      IntVector.Accessor accessor = rightJoinKey.getAccessor();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        if (leftValueMap.containsKey(accessor.get(i))) {
          int[] index = leftValueMap.lget();
          outRecords.add(new int[]{index[0], index[1], i});
        }
      }
      return !outRecords.isEmpty();
    }

  }

  // right outer join
  class RightConnector extends Connector {

    BitVector rightMarkBits = new BitVector(null, getContext().getAllocator());

    @Override
    public void upstream() {
      outputVectors.clear();
      List<Integer> misMatchIndex = Lists.newArrayList();
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
      ValueVector out;
      Mutator outMutator;
      for (MaterializedField f : leftSchema) {
        if (f.equals(keyField)) {
          continue;
        }
        out = TypeHelper.getNewVector(getMaterializedField(f), context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 8);
        outMutator = out.getMutator();
        ValueVector.Accessor current = null;
        int cursor = -1;
        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          if (indexes[0] != cursor) {
            cursor = indexes[0];
            current = getVector(f, leftIncomings.get(indexes[0])).getAccessor();
          }
          outMutator.setObject(i, current.getObject(indexes[1]));
        }
        for (int i = outRecords.size(); i < recordCount; i++) {
          outMutator.setObject(i, null);
        }
        outMutator.setValueCount(recordCount);
        outputVectors.add(out);
      }
      for (MaterializedField f : rightSchema) {
        out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 8);
        outMutator = out.getMutator();
        ValueVector.Accessor accessor = getVector(f, rightVectors).getAccessor();
        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, accessor.getObject(indexes[2]));
        }
        int index = outRecords.size();
        for (int i : misMatchIndex) {
          outMutator.setObject(index++, accessor.getObject(i));
        }
        outMutator.setValueCount(recordCount);
        outputVectors.add(out);
      }
      outRecords.clear();
    }

    private MaterializedField getMaterializedField(MaterializedField f) {
      return MaterializedField.create(new SchemaPath(f.getName(), ExpressionPosition.UNKNOWN)
        , Types.optional(f.getType().getMinorType()));
    }

    @Override
    public boolean connect() {
      rightMarkBits.allocateNew(rightValueCount);
      BitVector.Mutator mutator = rightMarkBits.getMutator();
      mutator.setValueCount(rightValueCount);
      IntVector.Accessor accessor = rightJoinKey.getAccessor();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        if (leftValueMap.containsKey(accessor.get(i))) {
          int[] index = leftValueMap.lget();
          outRecords.add(new int[]{index[0], index[1], i});
          mutator.set(i, 1);
        }
      }
      return true;
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
  }

  class LeftCache {
    BatchSchema schema;
    List<List<ValueVector>> incomings = Lists.newArrayList();
    IntObjectOpenHashMap<int[]> valuesIndexMap = new IntObjectOpenHashMap<>();
    MaterializedField keyField;

    public void cache(List<ValueVector> incoming, IntVector joinKey) {
      incomings.add(incoming);
      int index = incomings.size() - 1;
      IntVector.Accessor accessor = joinKey.getAccessor();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        valuesIndexMap.put(accessor.get(i), new int[]{index, i});
      }
      keyField = joinKey.getField();
      joinKey.close();
    }

    BatchSchema getSchema() {
      return schema;
    }

    void setSchema(BatchSchema schema) {
      this.schema = schema;
    }

    IntObjectOpenHashMap<int[]> getValuesIndexMap() {
      return valuesIndexMap;
    }

    List<List<ValueVector>> getIncomings() {
      return incomings;
    }

    public int size() {
      return incomings.size();
    }

    public void clear() {
      for (List<ValueVector> vectors : incomings) {
        for (ValueVector v : vectors) {
          v.close();
        }
      }
    }
  }

  class RightCache {
    BatchSchema schema;
    Deque<List<ValueVector>> incomings = new LinkedList<>();
    Deque<IntVector> joinKeys = new LinkedList<>();
    Deque<Integer> recordCounts = new LinkedList<>();

    public void cache(List<ValueVector> incoming, IntVector joinKey, int recordCount) {
      incomings.addLast(incoming);
      joinKeys.addLast(joinKey);
      recordCounts.addLast(recordCount);
    }

    public Tuple<List<ValueVector>, IntVector, Integer> removeFirst() {
      return new Tuple<>(incomings.removeFirst(),
        joinKeys.removeFirst(), recordCounts.removeFirst());
    }

    BatchSchema getSchema() {
      return schema;
    }

    void setSchema(BatchSchema schema) {
      this.schema = schema;
    }

    public int size() {
      return incomings.size();
    }

    public void clear() {
      for (List<ValueVector> vectors : incomings) {
        for (ValueVector v : vectors) {
          v.close();
        }
      }
      for (ValueVector v : joinKeys) {
        v.close();
      }
    }

    class Tuple<First, Second, Third> {

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
  }

}
