package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Group;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.vector.Fixed4;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

import java.util.*;
import org.apache.drill.exec.physical.config.Groupby;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:19 AM
 */
public class SegmentBatch extends BaseRecordBatch {

    private Group config;
    private FragmentContext context ;
    private RecordBatch incoming ;
    private BatchSchema batchSchema;
    private BasicEvaluator[] evaluators;
    private SchemaPath ref;
    private int groupTotal;
    private int groupByExprsLength;
    private int fieldId;
    private MaterializedField refField;

    private Map<Integer, List<Integer>> groups;
    private Map<GroupByExprsValue, Integer> groupInfo;

    public SegmentBatch(FragmentContext context, Group config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
        this.ref = new SchemaPath(config.getRef().getPath());
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
            evaluators[i] = evaluatorFactory.getBasicEvaluator(incoming.getRecordPointer(), logicalExpressions[i]);
        }
        groupByExprsLength = evaluators.length;
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
        incoming.kill();
    }

    @Override
    public IterOutcome next() {

        if (groups.size() != 0) {
            consume();
            return IterOutcome.OK;
        }

        IterOutcome o = incoming.next();

        switch (o) {
            case NONE:
            case STOP:
                recordCount = 0;
                fields.clear();
                break;

            case OK_NEW_SCHEMA:
                buildSchema();
            case OK:
                doGroup();
                consume();

        }
        return o;
    }

    private void buildSchema() {
        SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
        int maxFieldId = Integer.MIN_VALUE;
        for (MaterializedField f : incoming.getSchema()) {
            schemaBuilder.addField(f);
            maxFieldId = maxFieldId > f.getFieldId() ? maxFieldId : f.getFieldId();
        }

        fieldId = maxFieldId + 1;
        refField = MaterializedField.create(ref, fieldId, 0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.UINT4));
        schemaBuilder.addField(refField);
        try {
            batchSchema = schemaBuilder.build();
        } catch (SchemaChangeException e) {

        }
    }

    private void consume() {
        int groupId = groups.keySet().iterator().next();
        List<Integer> indexes = groups.remove(groupId);
        recordCount = indexes.size();
        BatchSchema incomingSchema = incoming.getSchema();
        ValueVector out;
        for (MaterializedField f : incomingSchema) {
            try {
                ValueVector in = incoming.getValueVector(f.getFieldId());
                out = TypeHelper.getNewVector(f, context.getAllocator());
                out.allocateNew(recordCount);
                out.setRecordCount(recordCount);

                for (int i = 0; i < recordCount; i++) {
                    out.setObject(i, in.getObject(indexes.get(i)));
                }
                fields.put(f.getFieldId(), out);

            } catch (InvalidValueAccessor e) {

            }
        }

        Fixed4 group = (Fixed4) TypeHelper.getNewVector(refField, context.getAllocator());
        group.allocateNew(1);
        group.setRecordCount(1);
        group.setInt(0, groupId);
        fields.put(fieldId, group);
        record.set(batchSchema.getFields(), fields);


    }

    private void doGroup() {
        ValueVector[] vectors = new ValueVector[groupByExprsLength];
        for (int i = 0; i < groupByExprsLength; i++) {
            vectors[i] = (ValueVector) evaluators[i].eval();
        }
        Object[] groupByExprs;
        GroupByExprsValue groupByExprsValue;
        recordCount = vectors[0].getRecordCount();
        for (int i = 0; i < recordCount; i++) {
            groupByExprs = new Object[groupByExprsLength];
            for (int j = 0; j < groupByExprsLength; j++) {
                groupByExprs[j] = vectors[j].getObject(i);
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
            for(int i = 0 ; i < exprValues.length ;i ++){
                if(exprValues[i] instanceof  byte[]){
                    exprValues[i] = new String((byte[]) exprValues[i]) ;
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
    }


}
