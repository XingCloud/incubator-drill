package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Distinct;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/10/13
 * Time: 5:49 PM
 */
public class DistinctBatch extends BaseRecordBatch {

    private FragmentContext context;
    private Distinct config ;
    private RecordBatch incoming;
    private BatchSchema batchSchema;

    public DistinctBatch(FragmentContext context, Distinct config, RecordBatch incoming) {
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
        return batchSchema;
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
