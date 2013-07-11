package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.vector.Bit;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/2/13
 * Time: 11:02 AM
 */
public class FilterBatch extends BaseRecordBatch {

    private FragmentContext context;
    private Filter config;
    private RecordBatch incoming;

    private BatchSchema batchSchema;
    private BooleanEvaluator eval;


    public FilterBatch(FragmentContext context, Filter config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
        setupEvals();
    }

    @Override
    public void setupEvals() {
        eval = new BasicEvaluatorFactory().getBooleanEvaluator(incoming.getRecordPointer(), config.getExpr());
    }

    @Override
    public FragmentContext getContext() {
        return context;
    }

    @Override
    public BatchSchema getSchema() {
        return incoming.getSchema();
    }

    @Override
    public void kill() {
        incoming.kill();
    }

    @Override
    public IterOutcome next() {

        IterOutcome o = incoming.next();
        switch (o) {
            case OK_NEW_SCHEMA:
                batchSchema = incoming.getSchema();
            case OK:
                recordCount = 0;
                int incomingSize = incoming.getRecordCount();
                Bit bitFilter = eval.eval();
                for (int i = 0; i < bitFilter.capacity(); i++) {
                    if (bitFilter.getBit(i) == 1) {
                        recordCount++;
                    }
                }
                for (MaterializedField f : batchSchema) {
                    try {
                        ValueVector in = incoming.getValueVector(f.getFieldId());
                        ValueVector out = TypeHelper.getNewVector(f, context.getAllocator());
                        out.setField(in.getField());
                        out.allocateNew(recordCount);
                        out.setRecordCount(recordCount);
                        for (int i = 0, j = 0; i < recordCount && j < in.getRecordCount(); j++) {
                            if (bitFilter.getBit(j) == 1) {
                                out.setObject(i++, in.getObject(j));
                            }
                        }
                        fields.put(f.getFieldId(), out);
                    } catch (Exception e) {

                    }
                }
                record.set(batchSchema.getFields(), fields);
                break;
            case NONE:
            case STOP:
                fields.clear();
                recordCount = 0;
        }
        return o;
    }

}
