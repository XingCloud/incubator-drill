package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.Maps;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private BatchSchema leftSchema;

  private List<List<ValueVector>> leftIncomings;
  private IntVector leftJoinKey ;
  private IntVector rightJoinKey;
  private IntObjectOpenHashMap<int[]> leftValueMap = new IntObjectOpenHashMap<>() ;
  private boolean leftCached = false;
  private boolean new_schema = true;

  private List<ValueVector> rightVectors;

  private Connector connector;

  private MaterializedField leftJoinKeyField;


  public JoinBatch(FragmentContext context, JoinPOP config, RecordBatch leftIncoming, RecordBatch rightIncoming) {
    this.context = context;
    this.config = config;
    this.leftIncoming = leftIncoming;
    this.rightIncoming = rightIncoming;
    this.leftIncomings = Lists.newArrayList();
    this.rightVectors = Lists.newArrayList();

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

    if (!leftCached) {
      leftCached = true;
      try {
        while(true){
          IterOutcome o = leftIncoming.next() ;
          if(o == IterOutcome.STOP)
            return  o ;
          if(o == IterOutcome.NONE)
            break;
          cacheLeft();
        }
        if (leftIncomings.isEmpty()) {
          return IterOutcome.NONE;
        }
      } catch (Exception e) {
        logger.error(e.getMessage());
        e.printStackTrace();
        context.fail(e);
        return IterOutcome.STOP;
      }
    }

    while (true) {
      IterOutcome o = rightIncoming.next();
      switch (o) {
        case NONE:
          clearLeft();
        case STOP:
        case NOT_YET:
          return o;
        case OK_NEW_SCHEMA:
          new_schema = true;
        case OK:
          try {
            if (!connector.connect()){
              clearRight();
              continue;
            }
            connector.upstream();
            clearRight();
            if (new_schema) {
              new_schema = false;
              setupSchema();
              return IterOutcome.OK_NEW_SCHEMA;
            }
          } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            context.fail(e);
            return IterOutcome.STOP;
          }
          return o;
      }
    }
  }

  private void cacheLeft() {
    leftSchema = leftIncoming.getSchema();
    leftJoinKey = (IntVector) leftEvaluator.eval();
    leftJoinKeyField = leftJoinKey.getField();
    leftIncomings.add(TransferHelper.transferVectors(leftIncoming));
    IntVector.Accessor accessor = leftJoinKey.getAccessor();
    int index = leftIncomings.size() - 1;
    for (int i = 0; i < accessor.getValueCount(); i++) {
      leftValueMap.put(accessor.get(i), new int[]{index, i});
    }
    leftJoinKey.close();
  }

  private void cacheRight() {
    rightJoinKey = (IntVector) rightEvaluator.eval();
    rightVectors.clear();
    rightVectors.addAll(TransferHelper.transferVectors(rightIncoming));
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
    clearLeft();
    clearRight();
    for (ValueVector v : outputVectors) {
      v.close();
    }
  }

  private void clearLeft() {
    for (List<ValueVector> vectors : leftIncomings) {
      for (ValueVector v : vectors) {
        v.close();
      }
    }
    if(leftJoinKey != null){
      leftJoinKey.close();
    }
  }

  private void clearRight() {
    for (ValueVector v : rightVectors) {
      v.close();
    }
    if (rightJoinKey != null) {
      rightJoinKey.close();
    }
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

    public abstract void upstream();

    public abstract boolean connect();

    public abstract void clear();
  }


  // inner join
  class InnerConnector extends Connector {

    List<int[]> outRecords = Lists.newArrayList();

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
        ValueVector.Accessor current = null  ;
        int cursor = -1 ;
        for (int i = 0; i < recordCount; i++) {
          int[] indexes = outRecords.get(i);
          if(indexes[0] != cursor){
            cursor = indexes[0] ;
            current = getVector(f, leftIncomings.get(indexes[0])).getAccessor() ;
          }
          outMutator.setObject(i, current.getObject(indexes[1]));
        }
        outMutator.setValueCount(recordCount);
        outputVectors.add(out);
      }
      for (MaterializedField f : rightIncoming.getSchema()) {
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
      cacheRight();
      IntVector.Accessor accessor = rightJoinKey.getAccessor() ;
      for (int i = 0; i < accessor.getValueCount(); i++) {
        if(leftValueMap.containsKey(accessor.get(i))){
          int[] index = leftValueMap.lget();
          outRecords.add(new int[]{index[0],index[1],i});
        }
      }
      return !outRecords.isEmpty();
    }

    @Override
    public void clear() {

    }
  }

  // right outer join
  class RightConnector extends Connector {

    BitVector rightMarkBits = new BitVector(null, getContext().getAllocator());
    List<int[]> outRecords = Lists.newArrayList();

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
        if (f.equals(leftJoinKeyField)) {
          continue;
        }
        out = TypeHelper.getNewVector(getMaterializedField(f), context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 8);
        outMutator = out.getMutator();
        ValueVector.Accessor current = null ;
        int cursor = -1 ;
        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          if(indexes[0] != cursor){
            cursor = indexes[0] ;
            current = getVector(f, leftIncomings.get(indexes[0])).getAccessor() ;
          }
          outMutator.setObject(i, current.getObject(indexes[1]));
        }
        for(int i = outRecords.size() ; i < recordCount ; i++){
          outMutator.setObject(i,null);
        }
        outMutator.setValueCount(recordCount);
        outputVectors.add(out);
      }
      for (MaterializedField f : rightIncoming.getSchema()) {
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
      cacheRight();
      rightMarkBits.allocateNew(rightIncoming.getRecordCount());
      BitVector.Mutator mutator = rightMarkBits.getMutator();
      mutator.setValueCount(rightIncoming.getRecordCount());

      IntVector.Accessor accessor = rightJoinKey.getAccessor();
      for (int i = 0; i < accessor.getValueCount(); i++) {
        if(leftValueMap.containsKey(accessor.get(i))){
          int[] index = leftValueMap.lget() ;
          outRecords.add(new int[]{index[0], index[1], i});
          mutator.set(i, 1);
        }
      }
      return true;
    }

    @Override
    public void clear() {
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

}
