package org.apache.drill.exec.physical.impl;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Join;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.vector.ValueVector;

import java.util.List;

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

    private List<IntObjectOpenHashMap<ValueVector<?>>> leftValeus ;

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
        IterOutcome o ;
        o = leftIncoming.next() ;
        while(o != IterOutcome.NONE){

        }
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
