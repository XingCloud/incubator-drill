package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:45 PM
 */
public interface EvaluatorTypes {

    public interface BasicEvaluator {
        public ValueVector eval();
    }

    public interface BooleanEvaluator {
        public BitVector eval();
    }

    public interface ConstantEvaluator {
        public ValueVector eval();
    }

    public interface AggregatingEvaluator extends BasicEvaluator {
        public void addBatch();
    }
}
