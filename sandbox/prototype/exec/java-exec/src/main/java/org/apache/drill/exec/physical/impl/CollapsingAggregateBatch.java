package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.CollapsingAggregate;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/11/13
 * Time: 1:53 PM
 */
public class CollapsingAggregateBatch extends BaseRecordBatch {

    private FragmentContext context;
    private CollapsingAggregate config ;
    private RecordBatch incoming ;

    public CollapsingAggregateBatch(FragmentContext context, CollapsingAggregate config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
    }

    @Override
    public void setupEvals() {

    }

    @Override
    public FragmentContext getContext() {
        return context;
    }

    @Override
    public BatchSchema getSchema() {
        return null;
    }

    @Override
    public void kill() {
       incoming.kill();
    }

    @Override
    public IterOutcome next() {
        return null;
    }
}
