package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.vector.Bit;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:45 PM
 */
public interface EvaluatorTypes {

    public interface BasicEvaluator{
        public DrillValue eval();
        public boolean isConstant();
    }

    public interface BooleanEvaluator{
        public Bit eval();
    }

    public interface ConstantEvaluator{
        public DrillValue eval();
    }

    public interface AggregatingEvaluator extends BasicEvaluator{
        public void addBatch();
    }
}
