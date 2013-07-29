package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 7:24 PM
 */
public abstract class BaseBasicEvaluator implements BasicEvaluator {
    private final boolean isConstant;
    protected final RecordBatch recordBatch;

    public BaseBasicEvaluator(boolean constant, RecordBatch recordBatch) {
        super();
        isConstant = constant;
        this.recordBatch = recordBatch;
    }
}
