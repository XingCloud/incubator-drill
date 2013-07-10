package org.apache.drill.exec.util.operation;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.values.NumericValue;
import org.apache.drill.exec.record.vector.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 10:53 PM
 */
public class Comparator {

    private final static byte POSITIVE = 1;
    private final static byte ZERO = 0;
    private final static byte NEGATIVE = -1;

    enum RELATION {
        EQUAL_TO, GREATER_THAN, LESS_THAN,
    }

    public static Bit Equal(DrillValue left, DrillValue right) {
        return transferFrom(compare(left, right), RELATION.EQUAL_TO, ZERO);
    }

    public static Bit GreaterThan(DrillValue left, DrillValue right) {
        return transferFrom(compare(left, right), RELATION.EQUAL_TO, POSITIVE);
    }

    public static Bit LessThan(DrillValue left, DrillValue right) {
        return transferFrom(compare(left, right), RELATION.EQUAL_TO, NEGATIVE);
    }

    public static Bit GreaterEqual(DrillValue left, DrillValue right) {
        return transferFrom(compare(left, right), RELATION.GREATER_THAN, NEGATIVE);
    }

    public static Bit LessEqual(DrillValue left, DrillValue right) {
        return transferFrom(compare(left, right), RELATION.LESS_THAN, POSITIVE);
    }

    private static Fixed1 compare(DrillValue left, DrillValue right) {
        if (left.isVector()) {
            if (right.isVector()) {
                return V2V((ValueVector) left, (ValueVector) right);
            }
            return V2O((ValueVector) left, right);
        } else {
            if (right.isVector()) {
                return O2V(left, (ValueVector) right);
            }
            return O2O(left, right);
        }

    }


    private static Fixed1 V2O(ValueVector left, DrillValue right) {
        Fixed1 v = O2V(right, left);
        for (int i = 0; i < v.getRecordCount(); i++) {
            v.setByte(i, (byte) -v.getByte(i));
        }
        return v;
    }

    private static Fixed1 V2V(ValueVector left, ValueVector right) {
        return null;
    }

    private static Fixed1 O2V(DrillValue left, ValueVector right) {
        Fixed1 v = new Fixed1(right.getField(), BufferAllocator.getAllocator(null));
        int recordCount = right.getRecordCount();
        v.allocateNew(recordCount);
        v.setRecordCount(recordCount);
        int j = 0;
        byte b = 0;
        if (left.isNumeric()) {
            NumericValue n = (NumericValue) left;
            switch (right.getMinorType()) {
                case INT:
                case UINT4:
                    long l = n.getAsLong();
                    Fixed4 ints = (Fixed4) right;
                    int i = 0;
                    for (j = 0; j < recordCount; j++) {
                        i = ints.getInt(j);
                        b = (byte) (l == i ? 0 : l > i ? 1 : -1);
                        v.setByte(j, b);
                    }
                    return v;
                case DECIMAL4:
                case FLOAT4:
                    double d = n.getAsDouble();
                    Fixed4 floats = (Fixed4) right;
                    float f;
                    for (j = 0; j < recordCount; j++) {
                        f = floats.getFloat4(j);
                        b = (byte) (d == f ? 0 : d > f ? 1 : -1);
                        v.setByte(j, b);
                    }
                    return v;
                case BIGINT:
                case UINT8:
                case DECIMAL8:
                    l = n.getAsLong();
                    Fixed8 longs = (Fixed8) right;
                    long bigInt ;
                    for (j = 0; j < recordCount; j++) {
                        bigInt = longs.getBigInt(j);
                        b = (byte) (l == bigInt ? 0 : l > bigInt ? 1 : -1);
                        v.setByte(j, b);
                    }
                    return v;
            }
        } else {
            // String
        }
        return null;
    }

    private static Fixed1 O2O(DrillValue left, DrillValue right) {
        Fixed1 fixed1 = new Fixed1(null,BufferAllocator.getAllocator(null));
        fixed1.allocateNew(1);
        fixed1.setRecordCount(1);
        // TODO
        return fixed1;
    }

    private static Bit transferFrom(Fixed1 fixed1, RELATION relation, byte value) {
        Bit bits = new Bit(null, BufferAllocator.getAllocator(null));
        int recordCount = fixed1.getRecordCount();
        bits.allocateNew(recordCount);
        bits.setRecordCount(recordCount);
        byte b;
        for (int i = 0; i < recordCount; i++) {
            b = fixed1.getByte(i);
            switch (relation) {
                case EQUAL_TO:
                    if (b == value) bits.set(i);
                    break;
                case GREATER_THAN:
                    if (b > value) bits.set(i);
                    break;
                case LESS_THAN:
                    if (b < value) bits.set(i);
            }
        }
        return bits;
    }
}
