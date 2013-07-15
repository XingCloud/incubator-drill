package org.apache.drill.exec.physical.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.PhysicalCollapsingAggregate;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorFactory;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.vector.ValueVector;

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
    private boolean hasMore = true ;


    private AggregatingEvaluator[] aggregatingEvaluators;
    private SchemaPath[] aggNames;

    public CollapsingAggregateBatch(FragmentContext context, PhysicalCollapsingAggregate config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
        setupEvals();

    }

    @Override
    public void setupEvals() {
        // TODO
        if (config.getWithin() != null) {

        }

        aggregatingEvaluators = new AggregatingEvaluator[config.getAggregations().length];
        aggNames = new SchemaPath[aggregatingEvaluators.length];
        EvaluatorFactory evaluatorFactory = new BasicEvaluatorFactory();
        for (int i = 0; i < aggregatingEvaluators.length; i++) {
            aggregatingEvaluators[i] = evaluatorFactory.
                    getAggregateEvaluator(record, config.getAggregations()[i].getExpr());
            aggNames[i] = config.getAggregations()[i].getRef();
        }
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
        if(!hasMore){
            fields.clear();
            recordCount = 0 ;
            return IterOutcome.NONE;
        }

        IterOutcome o = incoming.next();
        while (o != IterOutcome.NONE) {
            record.set(incoming.getRecordPointer().getFieldsInfo(),incoming.getRecordPointer().getFields());

            consumeCurrent();
            o = incoming.next();
        }
        ValueVector v;
        SchemaBuilder sb = BatchSchema.newBuilder();
        for (int i = 0; i < aggNames.length; i++) {
            v = (ValueVector) aggregatingEvaluators[i].eval();
            System.out.println(v.getObject(0));
            sb.addField(v.getField());
            fields.put(i, v);
        }
        try {
            batchSchema = sb.build();
        } catch (SchemaChangeException e) {

        }

        recordCount = 1;
        hasMore = false;
        return IterOutcome.OK_NEW_SCHEMA;
    }
}
