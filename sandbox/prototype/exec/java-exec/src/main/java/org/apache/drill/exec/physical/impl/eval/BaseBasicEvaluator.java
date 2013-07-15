package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.RecordPointer;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 7:24 PM
 */
public abstract class BaseBasicEvaluator implements BasicEvaluator {
    private final boolean isConstant;
    protected final RecordPointer record;

    public BaseBasicEvaluator(boolean constant, RecordPointer record) {
        super();
        isConstant = constant;
        this.record = record;
    }

    @Override
    public boolean isConstant() {
        return isConstant;
    }
}
