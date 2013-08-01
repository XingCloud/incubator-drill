package org.apache.drill.exec.physical.impl;

import com.beust.jcommander.internal.Lists;
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
  private JoinPOP config;
  private RecordBatch leftIncoming;
  private RecordBatch rightIncoming;
  private BatchSchema batchSchema;
  private BasicEvaluator leftEvaluator;
  private BasicEvaluator rightEvaluator;

  private List<List<ValueVector>> leftIncomings;
  private List<ValueVector> leftJoinKeys;
  private ValueVector rightJoinKey;
  private Map<Object, List<Integer>> rightValueMap = new HashMap<>();
  private boolean leftCached = false;
  private boolean isFirst = true;

  private List<ValueVector> rightVectors;

  private Connector connector;


  public JoinBatch(FragmentContext context, JoinPOP config, RecordBatch leftIncoming, RecordBatch rightIncoming) {
    this.context = context;
    this.config = config;
    this.leftIncoming = leftIncoming;
    this.rightIncoming = rightIncoming;
    this.leftIncomings = Lists.newArrayList();
    this.leftJoinKeys = Lists.newArrayList();
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

    while (true) {
      IterOutcome o = rightIncoming.next();
      switch (o) {
        case NONE:
        case STOP:
        case NOT_YET:
          return o;
        case OK_NEW_SCHEMA:
        case OK:
          if (!connector.connect())
            continue;
          connector.upstream();
          if (isFirst) {
            isFirst = false;
            buildSchema();
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
      leftJoinKeys.add(TransferHelper.mirrorVector(v));
      leftIncomings.add(TransferHelper.transferVectors(leftIncoming));

      o = leftIncoming.next();
    }

    return !leftIncomings.isEmpty();
  }

  private void cacheRight() {
    rightJoinKey = TransferHelper.mirrorVector(rightEvaluator.eval());
    rightValueMap.clear();
    ValueVector.Accessor accessor = rightJoinKey.getAccessor();
    for (int i = 0; i < accessor.getValueCount(); i++) {
      Object key = accessor.getObject(i);
      List<Integer> value = rightValueMap.get(key);
      if (value == null) {
        value = Lists.newArrayList();
        value.add(i);
        rightValueMap.put(key, value);
      } else {
        value.add(i);
      }
    }

    rightVectors.clear();
    rightVectors.addAll(TransferHelper.transferVectors(rightIncoming));
  }

  public void buildSchema() {

    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : outputVectors) {
      schemaBuilder.addField(v.getField());
    }
    batchSchema = schemaBuilder.build();
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

      for (MaterializedField f : leftIncoming.getSchema()) {
        out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 50);
        outMutator = out.getMutator();
        outMutator.setValueCount(recordCount);

        for (int i = 0; i < recordCount; i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, getVector(f, leftIncomings.get(indexes[0])).getAccessor().getObject(indexes[1]));
        }
        outputVectors.add(out);
      }

      for (MaterializedField f : rightIncoming.getSchema()) {

        out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 50);
        outMutator = out.getMutator();
        outMutator.setValueCount(recordCount);
        ValueVector.Accessor accessor = getVector(f, rightVectors).getAccessor();

        for (int i = 0; i < recordCount; i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, accessor.getObject(indexes[2]));
        }
        outputVectors.add(out);
      }

      outRecords.clear();
    }

    @Override
    public boolean connect() {
      cacheRight();
      for (int i = 0; i < leftJoinKeys.size(); i++) {
        Accessor leftKeyAccessor = leftJoinKeys.get(i).getAccessor();
        for (int j = 0; j < leftKeyAccessor.getValueCount(); j++) {
          List<Integer> indexs = rightValueMap.get(leftKeyAccessor.getObject(j));
          if (indexs != null) {
            for (int k : indexs) {
              outRecords.add(new int[]{i, j, k});
            }
          }
        }
      }
      return !outRecords.isEmpty();
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
      ValueVector out;
      Mutator outMutator;
      for (MaterializedField f : leftIncoming.getSchema()) {
        out = TypeHelper.getNewVector(getMaterializedField(f), context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 50);
        outMutator = out.getMutator();
        outMutator.setValueCount(recordCount);

        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, getVector(f, leftIncomings.get(indexes[0])).getAccessor().getObject(indexes[1]));
        }
        outputVectors.add(out);
      }


      for (MaterializedField f : rightIncoming.getSchema()) {

        out = TypeHelper.getNewVector(f, context.getAllocator());
        AllocationHelper.allocate(out, recordCount, 50);
        outMutator = out.getMutator();
        outMutator.setValueCount(recordCount);
        ValueVector.Accessor accessor = getVector(f, rightVectors).getAccessor();

        for (int i = 0; i < outRecords.size(); i++) {
          int[] indexes = outRecords.get(i);
          outMutator.setObject(i, accessor.getObject(indexes[2]));
        }

        int index = outRecords.size();
        for (int i : misMatchIndex) {
          outMutator.setObject(index++, accessor.getObject(i));
        }
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

      for (int i = 0; i < leftJoinKeys.size(); i++) {
        Accessor leftKeyAccessor = leftJoinKeys.get(i).getAccessor();
        for (int j = 0; j < leftKeyAccessor.getValueCount(); j++) {
          List<Integer> indexs = rightValueMap.get(leftKeyAccessor.getObject(j));
          if (indexs != null) {
            for (int k : indexs) {
              outRecords.add(new int[]{i, j, k});
              mutator.set(k, 1);
            }
          }
        }
      }


      return true;
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
  }

}
