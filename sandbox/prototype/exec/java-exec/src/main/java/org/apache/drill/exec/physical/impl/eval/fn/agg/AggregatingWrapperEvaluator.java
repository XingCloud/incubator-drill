package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.DrillValue;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/10/13
 * Time: 5:27 PM
 */
public class AggregatingWrapperEvaluator implements AggregatingEvaluator {

    private List<AggregatingEvaluator> args;
    private  BasicEvaluator topEvaluator ;

    public AggregatingWrapperEvaluator(BasicEvaluator topEvaluator, List<AggregatingEvaluator> args) {
        super();
        this.topEvaluator = topEvaluator;
        this.args = args;
    }

    @Override
    public void addBatch() {
        for(AggregatingEvaluator aggregatingEvaluator:args){
            aggregatingEvaluator.addBatch();
        }
    }

    @Override
    public DrillValue eval() {
        return topEvaluator.eval();
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
