package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.record.vector.Fixed4;
import org.apache.drill.exec.record.vector.Fixed8;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:55 AM
 */

@FunctionEvaluator("sum")
public class SumAggregator implements AggregatingEvaluator {

    private BasicEvaluator child;
    private long l = 0;
    private double d = 0;
    private boolean integer = true;

    public SumAggregator(RecordPointer record, FunctionArguments args) {
        child = args.getOnlyEvaluator();
    }

    @Override
    public void addBatch() {
        DrillValue dv = child.eval();
        if (dv.isVector()) {
            ValueVector vv = (ValueVector) dv;
            switch (vv.getField().getType().getMinorType()) {
                 // TODO
            }
        } else {

        }
    }

    @Override
    public DrillValue eval() {
        Fixed8 value = new Fixed8(null, BufferAllocator.getAllocator(null));
        value.allocateNew(1);
        value.setRecordCount(1);
        if (integer) {
            value.setBigInt(0, l);
        } else {
            value.setFloat8(0, d);
        }
        l = 0 ; d = 0;
        return value;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
