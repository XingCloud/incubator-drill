package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SegmentPOP;
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

    private FragmentContext context ;
    private SegmentPOP config ;
    private RecordBatch incoming ;
    private BatchSchema batchSchema;

    public SegmentBatch(FragmentContext context, SegmentPOP config, RecordBatch incoming) {
        this.context = context;
        this.config = config;
        this.incoming = incoming;
    }

    @Override
    public void setupEvals() {

    }

    @Override
    public FragmentContext getContext() {
        return null;
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
