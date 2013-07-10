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

    public static ValueVector Add(DrillValue left , DrillValue right){

        return null;
    }

    private static ValueVector V2V(ValueVector left ,ValueVector right){
        return null;
    }

    private  static ValueVector O2V(NumericValue left,ValueVector right){
        return null;
    }

    private  static ValueVector V2O(ValueVector left,NumericValue right){
        return O2V(right,left);
    }

    private static ValueVector O2O(NumericValue left,NumericValue right){
        return null;
    }
}
