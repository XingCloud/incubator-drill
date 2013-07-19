package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.PhysicalCollapsingAggregate;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.vector.Fixed4;
import org.apache.drill.exec.record.vector.Fixed8;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/11/13
 * Time: 1:53 PM
 */
public class CollapsingAggregateBatch extends BaseRecordBatch {

    private FragmentContext context;
    private PhysicalCollapsingAggregate config;
    private RecordBatch incoming;
    private BatchSchema batchSchema;
    private boolean hasMore = true;


    private AggregatingEvaluator[] aggregatingEvaluators;
    private SchemaPath[] aggNames;

    private BasicEvaluator[] carryovers;
    private SchemaPath[] carryoverNames;


    private SchemaPath boundaryPath[];
    private BasicEvaluator boundaryKey;

    private Map<Integer, AggValue> aggValues = new HashMap<>();
    private SchemaDefProtos.MajorType[] carryOverTypes;

    private int outColumnsLength;


    public CollapsingAggregateBatch(FragmentContext context, PhysicalCollapsingAggregate config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
        setupEvals();

    }

    @Override
    public void setupEvals() {

        EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();

        if (config.getWithin() != null) {
            boundaryPath = new SchemaPath[]{config.getWithin()};
            boundaryKey = evaluatorFactory.getBasicEvaluator(incoming.getRecordPointer(), config.getWithin());
        }

        aggregatingEvaluators = new AggregatingEvaluator[config.getAggregations().length];
        aggNames = new SchemaPath[aggregatingEvaluators.length];
        carryovers = new BasicEvaluator[config.getCarryovers().length];
        carryoverNames = new FieldReference[config.getCarryovers().length];
        carryOverTypes = new SchemaDefProtos.MajorType[carryovers.length];

        for (int i = 0; i < aggregatingEvaluators.length; i++) {
            aggregatingEvaluators[i] = evaluatorFactory.
                    getAggregateEvaluator(incoming.getRecordPointer(), config.getAggregations()[i].getExpr());
            aggNames[i] = config.getAggregations()[i].getRef();
        }


        for (int i = 0; i < carryovers.length; i++) {
            carryovers[i] = evaluatorFactory.getBasicEvaluator(incoming.getRecordPointer(), config.getCarryovers()[i]);
            carryoverNames[i] = config.getCarryovers()[i];
        }

        outColumnsLength = aggNames.length + carryoverNames.length;
    }

    private void consumeCurrent() {
        for (int i = 0; i < aggregatingEvaluators.length; i++) {
            aggregatingEvaluators[i].addBatch();
        }
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
        if (!hasMore) {
            fields.clear();
            recordCount = 0;
            return IterOutcome.NONE;
        }

        IterOutcome o = incoming.next();
        while (o != IterOutcome.NONE) {
            consumeCurrent();
            int groupId = 0;
            Object[] carryOverValue = null;
            Long[] aggValues = new Long[aggregatingEvaluators.length];
            for (int i = 0; i < aggregatingEvaluators.length; i++) {
                aggValues[i] = ((Fixed8) aggregatingEvaluators[i].eval()).getBigInt(0);
            }

            if (carryovers.length != 0) {
                carryOverValue = new Object[carryovers.length];
                ValueVector v;
                for (int i = 0; i < carryovers.length; i++) {
                    v = (ValueVector) carryovers[i].eval();
                    carryOverValue[i] = v.getObject(0);
                    carryOverTypes[i] = v.getField().getType();
                }
            }

            if (boundaryKey != null) {
                groupId = ((Fixed4) boundaryKey.eval()).getInt(0);
            }

            mergeOutput(groupId, aggValues, carryOverValue);
            o = incoming.next();
        }

        writeOutPut();
        hasMore = false;
        return IterOutcome.OK_NEW_SCHEMA;
    }


    private void writeOutPut() {

        SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
        MaterializedField f;
        ValueVector v;
        recordCount = aggValues.size();
        List<ValueVector> vectors = new ArrayList<>();

        int fieldId = 0;
        for (; fieldId < aggNames.length; fieldId++) {
            f = MaterializedField.create(aggNames[fieldId], fieldId, 0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.BIGINT));
            schemaBuilder.addField(f);
            v = TypeHelper.getNewVector(f, context.getAllocator());
            v.allocateNew(recordCount);
            v.setRecordCount(recordCount);
            vectors.add(v);
        }


        if (carryoverNames.length != 0) {
            for (int i = 0; i < carryoverNames.length; i++, fieldId++) {
                f = MaterializedField.create(carryoverNames[i], fieldId, 0, carryOverTypes[i]);
                schemaBuilder.addField(f);
                v = TypeHelper.getNewVector(f, context.getAllocator());
                v.allocateNew(recordCount);
                v.setRecordCount(recordCount);
                vectors.add(v);
            }
        }
        try {
            batchSchema = schemaBuilder.build();
        } catch (SchemaChangeException e) {
            e.printStackTrace();
        }

        int i = 0;
        for (AggValue aggValue : aggValues.values()) {

            // test
            System.out.println(aggValue);

            for (int j = 0; j < outColumnsLength; j++) {
                vectors.get(j).setObject(i, aggValue.getObject(j));
            }
            i++;
        }

        for (ValueVector vector : vectors) {
            fields.put(vector.getField().getFieldId(), vector);
        }


        record.set(batchSchema.getFields(), fields);

    }


    private void mergeOutput(int groupId, Long[] values, Object[] carryOvers) {

        AggValue val = aggValues.get(groupId);
        if (val == null) {
            aggValues.put(groupId, new AggValue(values, carryOvers));
        } else {
            Long[] aggVals = val.aggValues;
            for (int i = 0; i < aggVals.length; i++) {
                aggVals[i] += values[i];
            }
        }
    }

    class AggValue {
        Long[] aggValues;
        Object[] carryOvers;

        AggValue(Long[] aggValues, Object[] carryOvers) {
            this.aggValues = aggValues;
            this.carryOvers = carryOvers;
        }

        Object getObject(int i) {
            if (i < aggValues.length) {
                return aggValues[i];
            } else {
                return carryOvers[i - aggValues.length];
            }
        }

        @Override
        public String toString() {
            return "AggValue{" +
                    "aggValues=" + Arrays.toString(aggValues) +
                    ", carryOvers=" + Arrays.toString(carryOvers) +
                    '}';
        }
    }


}
