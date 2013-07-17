package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Join;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:57 AM
 */
public class JoinBatch extends BaseRecordBatch {
    private FragmentContext context ;
    private Join config ;
    private RecordBatch leftIncoming ;
    private RecordBatch rightIncoming ;
    private BatchSchema batchSchema ;

    public JoinBatch(FragmentContext context, Join config, RecordBatch leftIncoming, RecordBatch rightIncoming) {
        this.context = context;
        this.config = config;
        this.leftIncoming = leftIncoming;
        this.rightIncoming = rightIncoming;
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
        leftIncoming.kill();
        rightIncoming.kill();
    }

    @Override
    public IterOutcome next() {
        return null;
    }
}
