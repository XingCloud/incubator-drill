package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.PhysicalJoin;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
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
  private FragmentContext context;
  private PhysicalJoin config;
  private RecordBatch leftIncoming;
  private RecordBatch rightIncoming;
  private BatchSchema batchSchema;
  private BasicEvaluator leftEvaluator;
  private BasicEvaluator rightEvaluator;

  private List<List<ValueVector>> leftIncomings;
  private List<ValueVector> leftValues;
  private ValueVector rightValue;
  private Map<Object, Integer> rightValueMap = new HashMap<>();
  private List<int[]> outRecords = new ArrayList<>();
  private boolean leftCached = false;
  private boolean isFirst = true;

  private List<ValueVector> rightVectors;

  public JoinBatch(FragmentContext context, PhysicalJoin config, RecordBatch leftIncoming, RecordBatch rightIncoming) {
    this.context = context;
    this.config = config;
    this.leftIncoming = leftIncoming;
    this.rightIncoming = rightIncoming;
    leftIncomings = Lists.newArrayList();
    leftValues = Lists.newArrayList();
    rightVectors = Lists.newArrayList();
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
    leftIncoming.kill();
    rightIncoming.kill();
  }

  @Override
  public IterOutcome next() {

    if (!leftCached) {
      leftCached = true;
      if (!cacheLeft()) {
        return IterOutcome.NONE;
      }
    }

    // TODO
    // joinType : inner join , left outer join , right outer join ,   full join
    // join condition : expressions

    while (true) {
      IterOutcome o = rightIncoming.next();
      switch (o) {
        case NONE:
        case STOP:
        case NOT_YET:
          return o;
        case OK_NEW_SCHEMA:
        case OK:
          if (!innerJoin())
            continue;
          writeOutPut();
          buildSchema();
          if (isFirst) {
            isFirst = false;
            return IterOutcome.OK_NEW_SCHEMA;

          }
          return o;
      }

    }

  }

  private boolean cacheLeft() {
    IterOutcome o;
    o = leftIncoming.next();
    while (o != IterOutcome.NONE) {
      ValueVector v = leftEvaluator.eval();
      leftValues.add(mirror(v));
      leftIncomings.add(transferVectors());

      o = leftIncoming.next();
    }

    return !leftIncomings.isEmpty();
  }

  private void cacheRight() {
    rightValue = mirror( rightEvaluator.eval());
    rightValueMap.clear();
    ValueVector.Accessor accessor = rightValue.getAccessor();
    for (int i = 0; i < accessor.getValueCount(); i++) {
      rightValueMap.put(accessor.getObject(i), i);
    }

    rightVectors.clear();
    for (ValueVector v : rightIncoming) {
      TransferPair tp = v.getTransferPair();
      tp.transfer();
      rightVectors.add(tp.getTo());
    }


  }

  private boolean innerJoin() {
    int i = 0;
    Integer k;
    cacheRight();
    ValueVector.Accessor accessor;
    for (ValueVector v : leftValues) {
      accessor = v.getAccessor();
      for (int j = 0; j < accessor.getValueCount(); j++) {
        k = rightValueMap.get(accessor.getObject(j));
        if (k != null) {
          outRecords.add(new int[]{i, j, k});
        }
      }
      i++;
    }
    return !outRecords.isEmpty();
  }


  private void writeOutPut() {
    for(ValueVector v : outputVectors){
      v.close();
    }
    outputVectors.clear();
    recordCount = outRecords.size();
    ValueVector out;
    ValueVector.Mutator outMutator;

    for (MaterializedField f : leftIncoming.getSchema()) {
      out = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(out, recordCount, 50);
      outMutator = out.getMutator();
      outMutator.setValueCount(recordCount);

      for (int i = 0; i < recordCount; i++) {
        int[] indexes = outRecords.get(i);
        outMutator.setObject(i, getVector(f,leftIncomings.get(indexes[0])).getAccessor().getObject(indexes[1]));
      }
      outputVectors.add(out);
    }

    for (MaterializedField f : rightIncoming.getSchema()) {

      out = TypeHelper.getNewVector(f, context.getAllocator());
      AllocationHelper.allocate(out, recordCount, 50);
      outMutator = out.getMutator();
      outMutator.setValueCount(recordCount);


      for (int i = 0; i < recordCount; i++) {
        int[] indexes = outRecords.get(i);
        outMutator.setObject(i, getVector(f,rightVectors).getAccessor().getObject(indexes[2]));
      }
      outputVectors.add(out);
    }

    outRecords.clear();
  }


  private void buildSchema() {
    if (!isFirst)
      return;
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : this) {
      schemaBuilder.addField(v.getField());
    }
    batchSchema = schemaBuilder.build();
  }

  private List<ValueVector> transferVectors() {
    List<ValueVector> newVectors = Lists.newArrayList();
    for (ValueVector v : leftIncoming) {
      TransferPair tp = v.getTransferPair();
      tp.transfer();
      newVectors.add(tp.getTo());
    }
    return newVectors;
  }

  private ValueVector mirror(ValueVector v) {
    ValueVector vector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
    AllocationHelper.allocate(vector, v.getValueCapacity(), 50);
    ValueVector.Accessor accessor = v.getAccessor();
    ValueVector.Mutator mutator = vector.getMutator();
    for(int i = 0 ; i < accessor.getValueCount() ;i ++){
      mutator.setObject(i,accessor.getObject(i));
    }
    mutator.setValueCount(accessor.getValueCount());
    return vector;
  }

  private ValueVector getVector(MaterializedField f,List<ValueVector> vectors){
    for(ValueVector vector : vectors){
      if(vector.getField().equals(f)){
        return vector;
      }
    }
    return null;
  }
}
