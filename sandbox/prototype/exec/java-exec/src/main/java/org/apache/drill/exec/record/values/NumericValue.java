package org.apache.drill.exec.record.values;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 4:24 PM
 */
public abstract class  NumericValue  implements DrillValue,BasicEvaluator{
    static  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NumericValue.class);


    public long getAsLong(){
       throw new DrillRuntimeException("");
    }

    public int getAsInt(){
        throw new DrillRuntimeException("");
    }

    public float getAsFloat(){
        throw new DrillRuntimeException("");
    }

    public double getAsDouble(){
        throw new DrillRuntimeException("");
    }

    public BigInteger getAsBigInteger(){
        throw new DrillRuntimeException("");
    }

    public BigDecimal getAsBigDecimal(){
        throw new DrillRuntimeException("");
    }

    @Override
    public boolean isVector() {
        return false;
    }

    @Override
    public boolean isNumeric() {
        return true;
    }

    @Override
    public DrillValue eval() {
        return this;
    }
}
