package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Group;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:19 AM
 */
public class SegmentBatch extends BaseRecordBatch {

  private Group config;
  private FragmentContext context;
  private RecordBatch incoming;
  private BatchSchema outSchema;
  private BasicEvaluator[] evaluators;
  private int groupTotal;
  private int groupByExprsLength;

  private Map<Integer, List<Integer>> groups;
  private Map<GroupByExprsValue, Integer> groupInfo;
  private IntVector refVector;
  private boolean isFirst;

  public SegmentBatch(FragmentContext context, Group config, RecordBatch incoming) {
    this.context = context;
    this.config = config;
    this.incoming = incoming;
    this.groupInfo = new HashMap<>();
    this.groups = new HashMap<>();
    this.groupTotal = 0;
    setupEvals();
  }

  @Override
  public void setupEvals() {
    LogicalExpression[] logicalExpressions = config.getExprs();
    evaluators = new BasicEvaluator[logicalExpressions.length];
    EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
    for (int i = 0; i < evaluators.length; i++) {
      evaluators[i] = evaluatorFactory.getBasicEvaluator(incoming, logicalExpressions[i]);
    }
    groupByExprsLength = evaluators.length;

    refVector = new IntVector(MaterializedField.create(new SchemaPath(
      config.getRef().getPath(), ExpressionPosition.UNKNOWN),
      Types.required(TypeProtos.MinorType.INT))
      , context.getAllocator());


  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return outSchema;
  }

  @Override
  public void kill() {
    incoming.kill();
  }

  @Override
  public IterOutcome next() {

    if (groups.size() != 0) {
      writeOutput();
      return IterOutcome.OK;
    }

    IterOutcome o = incoming.next();

    switch (o) {
      case NONE:
      case STOP:
      case NOT_YET:
        break;

      case OK_NEW_SCHEMA:
        isFirst = true;
      case OK:
        grouping();
        writeOutput();
        buildSchema();

    }
    return o;
  }

  private void buildSchema() {
    if (!isFirst)
      return;
    isFirst = false;
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : this) {
      schemaBuilder.addField(v.getField());
    }
    outSchema = schemaBuilder.build();
  }

  private void writeOutput() {
    for (ValueVector v : outputVectors) {
      v.close();
    }
    outputVectors.clear();
    int groupId = groups.keySet().iterator().next();
    List<Integer> indexes = groups.remove(groupId);
    recordCount = indexes.size();
    ValueVector out;
    ValueVector.Accessor inAccessor;
    ValueVector.Mutator outMutator;
    for (ValueVector in : incoming) {

      inAccessor = in.getAccessor();
      out = TypeHelper.getNewVector(in.getField(), context.getAllocator());
      AllocationHelper.allocate(out, recordCount, 50);
      outMutator = out.getMutator();
      outMutator.setValueCount(recordCount);

      for (int i = 0; i < recordCount; i++) {
        outMutator.setObject(i, inAccessor.getObject(indexes.get(i)));
      }
      outputVectors.add(out);
    }

    refVector.allocateNew(1);
    refVector.getMutator().setValueCount(1);
    refVector.getMutator().set(0, groupId);
    outputVectors.add(refVector);
  }

  private void grouping() {
    ValueVector[] vectors = new ValueVector[groupByExprsLength];
    for (int i = 0; i < groupByExprsLength; i++) {
      vectors[i] = evaluators[i].eval();
    }
    Object[] groupByExprs;
    GroupByExprsValue groupByExprsValue;
    recordCount = vectors[0].getAccessor().getValueCount();
    for (int i = 0; i < recordCount; i++) {
      groupByExprs = new Object[groupByExprsLength];
      for (int j = 0; j < groupByExprsLength; j++) {
        groupByExprs[j] = vectors[j].getAccessor().getObject(i);
      }
      groupByExprsValue = new GroupByExprsValue(groupByExprs);
      Integer groupNum = groupInfo.get(groupByExprsValue);
      if (groupNum == null) {
        groupNum = ++groupTotal;
        groupInfo.put(groupByExprsValue, groupNum);
      }

      List<Integer> group = groups.get(groupNum);
      if (group == null) {
        group = new LinkedList<>();
        group.add(i);
        groups.put(groupNum, group);
      } else {
        group.add(i);
      }
    }
  }

  class GroupByExprsValue {

    Object[] exprValues;

    GroupByExprsValue(Object[] exprValues) {
      this.exprValues = exprValues;
      for (int i = 0; i < exprValues.length; i++) {
        if (exprValues[i] instanceof byte[]) {
          exprValues[i] = new String((byte[]) exprValues[i]);
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GroupByExprsValue)) return false;

      GroupByExprsValue that = (GroupByExprsValue) o;
      if (!Arrays.equals(exprValues, that.exprValues)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return exprValues != null ? Arrays.hashCode(exprValues) : 0;
    }

    @Override
    public String toString() {
      return Arrays.toString(exprValues) ;
    }
  }


}
