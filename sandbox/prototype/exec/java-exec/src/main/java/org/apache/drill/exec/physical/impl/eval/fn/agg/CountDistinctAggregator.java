package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.DrillValue;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:53 AM
 */

@FunctionEvaluator("countDistinct")
public class CountDistinctAggregator implements AggregatingEvaluator{

    @Override
    public void addBatch() {

    }

    @Override
    public DrillValue eval() {
        return null;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
