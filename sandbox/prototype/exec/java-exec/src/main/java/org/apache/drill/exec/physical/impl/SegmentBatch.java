package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SegmentPOP;
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

  private SegmentPOP config;
  private FragmentContext context;
  private RecordBatch incoming;
  private BatchSchema outSchema;
  private BasicEvaluator[] evaluators;
  private int groupTotal;
  private int groupByExprsLength;

  private Map<Integer, List<Integer>> groups;
  private Map<GroupByExprsValue, Integer> groupInfo;
  private IntVector refVector;
  private SchemaPath[] groupRefs;
  private MajorType[] groupRefsTypes;
  private ValueVector[] evalValues;
  private boolean isFirst;

  public SegmentBatch(FragmentContext context, SegmentPOP config, RecordBatch incoming) {
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
    NamedExpression[] namedExpressions = config.getExprs();
    evaluators = new BasicEvaluator[namedExpressions.length];
    groupRefs = new SchemaPath[namedExpressions.length];
    groupRefsTypes = new MajorType[namedExpressions.length];
    EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
    for (int i = 0; i < evaluators.length; i++) {
      evaluators[i] = evaluatorFactory.getBasicEvaluator(incoming, namedExpressions[i].getExpr());
      groupRefs[i] = new SchemaPath(namedExpressions[i].getRef().getPath(), ExpressionPosition.UNKNOWN);
    }
    groupByExprsLength = evaluators.length;

    refVector = new IntVector(MaterializedField.create(new SchemaPath(
      config.getRef().getPath(), ExpressionPosition.UNKNOWN),
      Types.required(MinorType.INT))
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
        setupSchema();

    }
    return o;
  }

  private void setupSchema() {
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

      for (int i = 0; i < recordCount; i++) {
        outMutator.setObject(i, inAccessor.getObject(indexes.get(i)));
      }
      outMutator.setValueCount(recordCount);
      outputVectors.add(out);
    }

    refVector.allocateNew(1);
    refVector.getMutator().set(0, groupId);
    refVector.getMutator().setValueCount(1);

    for (int i = 0; i < groupByExprsLength; i++) {
      ValueVector v = TypeHelper.getNewVector(MaterializedField.create(groupRefs[i], groupRefsTypes[i]), context.getAllocator());
      AllocationHelper.allocate(v, 1, 50);
      v.getMutator().setObject(0, evalValues[i].getAccessor().getObject(indexes.get(0)));
      v.getMutator().setValueCount(1);
      outputVectors.add(v);
    }

    outputVectors.add(refVector);
  }

  private void grouping() {
    evalValues = new ValueVector[groupByExprsLength];
    for (int i = 0; i < groupByExprsLength; i++) {
      evalValues[i] = evaluators[i].eval();
      groupRefsTypes[i] = evalValues[i].getField().getType();
    }
    Object[] groupByExprs;
    GroupByExprsValue groupByExprsValue;
    recordCount = evalValues[0].getAccessor().getValueCount();
    for (int i = 0; i < recordCount; i++) {
      groupByExprs = new Object[groupByExprsLength];
      for (int j = 0; j < groupByExprsLength; j++) {
        groupByExprs[j] = evalValues[j].getAccessor().getObject(i);
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
      return Arrays.toString(exprValues);
    }
  }


}
