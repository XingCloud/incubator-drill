package org.apache.drill.exec.physical.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.PhysicalJoin;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;


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

    private List<IntObjectOpenHashMap<ValueVector<?>>> leftIncomings;
    private List<ValueVector> leftValues;
    private ValueVector rightValue;
    private Map<Object, Integer> rightValueMap = new HashMap<>();
    private List<int[]> outRecords = new ArrayList<>();
    private boolean leftCached = false;
    private boolean isFirst = true;

    private HashMap<Integer, MaterializedField> leftFieldToNewField = new HashMap<>();
    private HashMap<Integer, MaterializedField> rightFieldToNewField = new HashMap<>();

    public JoinBatch(FragmentContext context, PhysicalJoin config, RecordBatch leftIncoming, RecordBatch rightIncoming) {
        this.context = context;
        this.config = config;
        this.leftIncoming = leftIncoming;
        this.rightIncoming = rightIncoming;
        leftIncomings = new ArrayList<>();
        leftValues = new ArrayList<>();
        setupEvals();
    }

    @Override
    public void setupEvals() {
        JoinCondition condition = config.getConditoin();
        EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
        leftEvaluator = evaluatorFactory.getBasicEvaluator(leftIncoming.getRecordPointer(), condition.getLeft());
        rightEvaluator = evaluatorFactory.getBasicEvaluator(rightIncoming.getRecordPointer(), condition.getRight());


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
                    recordCount = 0;
                    fields.clear();
                    return o;
                case OK_NEW_SCHEMA:
                    buildSchema();
                case OK:
                    rightValue = (ValueVector) rightEvaluator.eval();
                    if (!doJoin())
                        continue;
                    writeOutPut();
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
            leftIncomings.add(cloneFileds());
            ValueVector v = (ValueVector) leftEvaluator.eval();
            leftValues.add(cloneVector(v));
            o = leftIncoming.next();
        }

        return !leftIncomings.isEmpty();
    }

    private boolean doJoin() {
        int i = 0;
        Integer k;
        rightValueVectorToMap();
        for (ValueVector v : leftValues) {
            for (int j = 0; j < v.getRecordCount(); j++) {
                k = rightValueMap.get(v.getObject(j));
                if (k != null) {
                    outRecords.add(new int[]{i, j, k});
                }
            }
            i++;
        }
        return !outRecords.isEmpty();
    }

    private void rightValueVectorToMap() {
        rightValueMap.clear();
        for (int i = 0; i < rightValue.getRecordCount(); i++) {
            rightValueMap.put(rightValue.getObject(i), i);
        }

    }

    private void writeOutPut() {
        recordCount = outRecords.size();
        ValueVector v;
        for (MaterializedField f : leftIncoming.getSchema()) {
            v = fields.get(leftFieldToNewField.get(f.getFieldId()).getFieldId());
            if (v == null) {
                v = TypeHelper.getNewVector(leftFieldToNewField.get(f.getFieldId()), context.getAllocator());
                v.allocateNew(recordCount);
            }
            if (recordCount > v.capacity()) {
                v.allocateNew(recordCount);
            }

            v.setRecordCount(recordCount);
            for (int i = 0; i < recordCount; i++) {
                int[] indexes = outRecords.get(i);
                v.setObject(i, leftIncomings.get(indexes[0]).get(f.getFieldId()).getObject(indexes[1]));
            }
            fields.put(v.getField().getFieldId(), v);
        }

        IntObjectOpenHashMap<ValueVector<?>> rightFields = rightIncoming.getRecordPointer().getFields();
        for (MaterializedField f : rightIncoming.getSchema()) {
            v = fields.get(rightFieldToNewField.get(f.getFieldId()).getFieldId());
            if (v == null) {
                v = TypeHelper.getNewVector(rightFieldToNewField.get(f.getFieldId()), context.getAllocator());
                v.allocateNew(recordCount);
            }
            if (recordCount > v.capacity()) {
                v.allocateNew(recordCount);
            }

            v.setRecordCount(recordCount);

            for (int i = 0; i < recordCount; i++) {
                int[] indexes = outRecords.get(i);
                v.setObject(i, rightFields.get(f.getFieldId()).getObject(indexes[2]));
            }
            fields.put(v.getField().getFieldId(), v);
        }

        outRecords.clear();
        record.set(batchSchema.getFields(), fields);
    }


    private void buildSchema() {

        SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
        int fieldId = 0;
        MaterializedField nf;
        for (MaterializedField f : leftIncoming.getSchema()) {
            nf = MaterializedField.create(new SchemaPath(f.getName()), fieldId, 0, f.getType());
            schemaBuilder.addField(nf);
            leftFieldToNewField.put(f.getFieldId(), nf);
            fieldId++;
        }
        for (MaterializedField f : rightIncoming.getSchema()) {
            nf = MaterializedField.create(new SchemaPath(f.getName()), fieldId, 0, f.getType());
            schemaBuilder.addField(nf);
            rightFieldToNewField.put(f.getFieldId(), nf);
            fieldId++;
        }

        try {
            batchSchema = schemaBuilder.build();
        } catch (SchemaChangeException e) {

        }

    }

    private IntObjectOpenHashMap<ValueVector<?>> cloneFileds() {
        IntObjectOpenHashMap<ValueVector<?>> newFields = new IntObjectOpenHashMap<>();
        IntObjectOpenHashMap<ValueVector<?>> fields = leftIncoming.getRecordPointer().getFields();
        for (MaterializedField f : leftIncoming.getRecordPointer().getFieldsInfo()) {
            newFields.put(f.getFieldId(), cloneVector(fields.get(f.getFieldId())));
        }

        return newFields;
    }

    private ValueVector cloneVector(ValueVector v) {
        ValueVector vector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
        vector.allocateNew(v.capacity());
        vector.setRecordCount(v.getRecordCount());
        for (int i = 0; i < vector.getRecordCount(); i++) {
            vector.setObject(i, v.getObject(i));
        }
        return vector;
    }
}
