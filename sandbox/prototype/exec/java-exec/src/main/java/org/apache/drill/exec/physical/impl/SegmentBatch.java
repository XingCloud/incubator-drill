package org.apache.drill.exec.physical.impl;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:19 AM
 */
public class SegmentBatch extends BaseRecordBatch {

  final static Logger logger = LoggerFactory.getLogger(SegmentBatch.class);

  private SegmentPOP config;
  private FragmentContext context;
  private RecordBatch incoming;
  private BatchSchema outSchema;
  private BasicEvaluator[] evaluators;
  private int groupTotal;
  private int groupByExprsLength;

  private IntObjectOpenHashMap<IntArrayList> groups;
  private ObjectIntOpenHashMap<GroupByExprsValue> groupInfo;
  private IntVector refVector;
  private SchemaPath[] groupRefs;
  private MajorType[] groupRefsTypes;
  private ValueVector[] segmentValues;
  private boolean newSchema = false;

  public SegmentBatch(FragmentContext context, SegmentPOP config, RecordBatch incoming) {
    this.context = context;
    this.config = config;
    this.incoming = incoming;
    this.groupInfo = new ObjectIntOpenHashMap<>();
    this.groups = new IntObjectOpenHashMap<>();
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
    releaseAssets();
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
        newSchema = true;
      case OK:
        try {
          grouping();
          writeOutput();
          setupSchema();
        } catch (Exception e) {
          logger.error(e.getMessage());
          e.printStackTrace();
          context.fail(e);
          return IterOutcome.STOP;
        }
    }
    return o;
  }

  private void setupSchema() {
    if (!newSchema)
      return;
    newSchema = false;
    SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
    for (ValueVector v : this) {
      schemaBuilder.addField(v.getField());
    }
    outSchema = schemaBuilder.build();
  }

  private void writeOutput() {
    outputVectors.clear();
    int groupId = groups.keys().iterator().next().value  ;
    IntArrayList indexes = groups.remove(groupId);
    recordCount = indexes.size();
    ValueVector out;
    ValueVector.Accessor inAccessor;
    ValueVector.Mutator outMutator;
    for (ValueVector in : incoming) {
      inAccessor = in.getAccessor();
      out = TypeHelper.getNewVector(in.getField(), context.getAllocator());
      AllocationHelper.allocate(out, recordCount, 8);
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
      AllocationHelper.allocate(v, 1, 8);
      v.getMutator().setObject(0, segmentValues[i].getAccessor().getObject(indexes.get(0)));
      v.getMutator().setValueCount(1);
      outputVectors.add(v);
    }
    outputVectors.add(refVector);
    if(groups.isEmpty()){
      clearSegment();
      clearIncoming();
    }
  }

  private void grouping() {
    segmentValues = new ValueVector[groupByExprsLength];
    for (int i = 0; i < groupByExprsLength; i++) {
      segmentValues[i] = evaluators[i].eval();
      groupRefsTypes[i] = segmentValues[i].getField().getType();
    }
    Object[] groupByExprs;
    GroupByExprsValue groupByExprsValue;
    recordCount = segmentValues[0].getAccessor().getValueCount();
    for (int i = 0; i < recordCount; i++) {
      groupByExprs = new Object[groupByExprsLength];
      for (int j = 0; j < groupByExprsLength; j++) {
        groupByExprs[j] = segmentValues[j].getAccessor().getObject(i);
      }
      groupByExprsValue = new GroupByExprsValue(groupByExprs);
      Integer groupNum = groupInfo.get(groupByExprsValue);
      if (groupNum == null) {
        groupNum = ++groupTotal;
        groupInfo.put(groupByExprsValue, groupNum);
      }
      IntArrayList group = groups.get(groupNum);
      if (group == null) {
        group = new IntArrayList();
        group.add(i);
        groups.put(groupNum, group);
      } else {
        group.add(i);
      }
    }
  }

  @Override
  public void releaseAssets() {
    for (ValueVector v : outputVectors) {
      v.close();
    }
    clearSegment();
    clearRef();
  }

  private void clearSegment(){
    if (segmentValues != null) {
      for (int i = 0; i < segmentValues.length; i++) {
        if (segmentValues[i] != null) {
          segmentValues[i].close();
        }
      }
    }
  }

  private void clearRef() {
    if (refVector != null) {
      refVector.close();
      refVector = null;
    }
  }

  private void clearIncoming() {
    for (ValueVector v : incoming) {
      v.close();
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
