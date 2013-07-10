package org.apache.drill.exec.util.operation;

import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.values.NumericValue;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 10:53 PM
 */
public class Adder {

    public static ValueVector Add(DrillValue left, DrillValue right) {
        if (left.isVector()) {
            if (right.isVector()) {
                return V2V((ValueVector) left, (ValueVector) right);
            } else {
                return V2O((ValueVector) left, (NumericValue) right);
            }
        } else {
            if (right.isVector()) {
                return V2O((ValueVector) right, (NumericValue) left);
            } else {
                return O2O((NumericValue) left, (NumericValue) right);
            }
        }
    }

    private static ValueVector V2V(ValueVector left, ValueVector right) {
        return null;
    }


    private static ValueVector V2O(ValueVector left, NumericValue right) {
        return null;
    }

    private static ValueVector O2O(NumericValue left, NumericValue right) {
        return null;
    }


}
