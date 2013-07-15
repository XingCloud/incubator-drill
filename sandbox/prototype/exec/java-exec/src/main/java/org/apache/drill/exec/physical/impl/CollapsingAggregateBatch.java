package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.PhysicalCollapsingAggregate;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;

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
    private BatchSchema schema;
    private boolean hasValue = false;

    private AggregatingEvaluator[] aggregatingEvaluators;
    private SchemaPath[] aggNames;

    public CollapsingAggregateBatch(FragmentContext context, PhysicalCollapsingAggregate config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
    }

    @Override
    public void setupEvals() {
        // TODO
        if (config.getWithin() != null) {

        }

        aggregatingEvaluators = new AggregatingEvaluator[config.getAggregations().length];
        aggNames = new SchemaPath[aggregatingEvaluators.length];
        for (int i = 0; i < aggregatingEvaluators.length; i++) {
            aggregatingEvaluators[i] = new BasicEvaluatorFactory().
                    getAggregateEvaluator(record, config.getAggregations()[i].getExpr());
            aggNames[i] = config.getAggregations()[i].getRef();
        }
    }

    private void consumeCurrent(){
        for(int i = 0 ; i < aggregatingEvaluators.length ; i++){
            aggregatingEvaluators[i].addBatch();
        }
    }

    @Override
    public FragmentContext getContext() {
        return context;
    }

    @Override
    public BatchSchema getSchema() {
        return schema;
    }

    @Override
    public void kill() {
        incoming.kill();
    }

    @Override
    public IterOutcome next() {
        IterOutcome o = incoming.next();
        while (o != IterOutcome.NONE) {

        }

        return IterOutcome.OK_NEW_SCHEMA;
    }
}
