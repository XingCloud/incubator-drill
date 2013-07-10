package org.apache.drill.exec.record.values;

import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.DrillValue;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 4:30 PM
 */
public class ScalarValues {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScalarValues.class);



    public static class IntegerScalar extends NumericValue{
        int i ;

        public IntegerScalar(int i) {
            this.i = i;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public long getAsLong() {
            return i;
        }

        @Override
        public BigDecimal getAsBigDecimal() {
            return BigDecimal.valueOf(i);
        }

        @Override
        public BigInteger getAsBigInteger() {
            return BigInteger.valueOf(i);
        }

        @Override
        public double getAsDouble() {
            return i;
        }

        @Override
        public float getAsFloat() {
            return i;
        }

        @Override
        public int getAsInt() {
            return super.getAsInt();
        }

        @Override
        public SchemaDefProtos.MinorType getMinorType() {
            return SchemaDefProtos.MinorType.BIGINT;
        }
    }

    public static class LongScalar extends NumericValue{

        long l ;

        public LongScalar(long l) {
            this.l = l;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public long getAsLong() {
            return l;
        }

        @Override
        public float getAsFloat() {
            return l;
        }

        @Override
        public double getAsDouble() {
            return l;
        }

        @Override
        public BigInteger getAsBigInteger() {
            return BigInteger.valueOf(l);
        }

        @Override
        public BigDecimal getAsBigDecimal() {
            return BigDecimal.valueOf(l);
        }

        @Override
        public SchemaDefProtos.MinorType getMinorType() {
            return  SchemaDefProtos.MinorType.BIGINT;
        }
    }

    public static class FolatScalar extends NumericValue{
        float f ;

        public FolatScalar(float f) {
            this.f = f;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public float getAsFloat() {
            return f;
        }

        @Override
        public double getAsDouble() {
            return f;
        }

        @Override
        public BigDecimal getAsBigDecimal() {
            return BigDecimal.valueOf(f);
        }

        @Override
        public SchemaDefProtos.MinorType getMinorType() {
            return SchemaDefProtos.MinorType.FLOAT4;
        }
    }

    public static class DoubleScalar extends NumericValue{
        double d;

        public DoubleScalar(double d) {
            this.d = d;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public BigDecimal getAsBigDecimal() {
            return BigDecimal.valueOf(d);
        }

        @Override
        public double getAsDouble() {
            return d;
        }

        @Override
        public SchemaDefProtos.MinorType getMinorType() {
            return SchemaDefProtos.MinorType.FLOAT8;
        }
    }

    public static class BooleanScalar implements BooleanValue, EvaluatorTypes.BasicEvaluator{
        boolean b ;

        public BooleanScalar(boolean b) {
            this.b = b;
        }

        @Override
        public DrillValue eval() {
            return this;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public boolean getBoolean() {
            return b;
        }

        @Override
        public boolean isVector() {
            return false;
        }

        @Override
        public SchemaDefProtos.MinorType getMinorType() {
            return SchemaDefProtos.MinorType.BOOLEAN;
        }

        @Override
        public boolean isNumeric() {
            return false;
        }
    }

    public static class StringScalar implements StringValue,EvaluatorTypes.BasicEvaluator{
        CharSequence seq;

        public StringScalar(CharSequence seq) {
            this.seq = seq;
        }

        @Override
        public DrillValue eval() {
            return this;
        }

        @Override
        public boolean isConstant() {
            return true;
        }

        @Override
        public CharSequence getString() {
            return seq;
        }

        @Override
        public boolean isVector() {
            return false;
        }

        @Override
        public SchemaDefProtos.MinorType getMinorType() {
            return null;
        }

        @Override
        public boolean isNumeric() {
            return false;
        }
    }
}
