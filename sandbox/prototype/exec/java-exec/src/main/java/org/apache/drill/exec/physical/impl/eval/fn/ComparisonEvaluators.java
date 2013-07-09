package org.apache.drill.exec.physical.impl.eval.fn;

import io.netty.buffer.ByteBufAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.record.vector.Bit;
import org.apache.drill.exec.record.vector.Fixed1;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/8/13
 * Time: 11:04 AM
 */
public class ComparisonEvaluators {

    private abstract static class ComparisonEvaluator extends BaseBasicEvaluator {
        private final BasicEvaluator left;
        private final BasicEvaluator right;

        public ComparisonEvaluator(RecordPointer record, FunctionArguments args) {
            super(args.isOnlyConstants(), record);
            this.left = args.getEvaluator(0);
            this.right = args.getEvaluator(1);

        }

        @Override
        public DrillValue eval() {
            DrillValue lv = left.eval();
            DrillValue lr = right.eval();
            Fixed1 fixed1 = (Fixed1) lv.compareTo(lr);
            Bit bits = new Bit(null, BufferAllocator.getAllocator(null));
            bits.allocateNew(fixed1.getRecordCount());
            bits.setRecordCount(fixed1.getRecordCount());
            for (int i = 0; i < fixed1.getRecordCount(); i++) {
                if (valid(fixed1.getByte(i)))
                    bits.set(i);
            }
            return bits;
        }

        public abstract boolean valid(byte b);
    }

    @FunctionEvaluator("equal")
    public static class EqualEvaluator extends ComparisonEvaluator{
        public EqualEvaluator(RecordPointer record, FunctionArguments args) {
            super(record, args);
        }

        @Override
        public boolean valid(byte b) {
            return b == 0;
        }
    }

    @FunctionEvaluator("greater than")
    public static class GreaterThan extends ComparisonEvaluator {

        public GreaterThan(RecordPointer record, FunctionArguments args) {
            super(record, args);
        }

        @Override
        public boolean valid(byte b) {
            return b > 0;
        }
    }

    @FunctionEvaluator("less than")
    public static class LessThan extends ComparisonEvaluator {

        public LessThan(RecordPointer record, FunctionArguments args) {
            super(record, args);
        }

        @Override
        public boolean valid(byte i) {
            return i < 0;
        }

    }

    @FunctionEvaluator("less than or equal to")
    public static class LessThanOrEqualTo extends ComparisonEvaluator {

        public LessThanOrEqualTo(RecordPointer record, FunctionArguments args) {
            super(record, args);
        }

        @Override
        public boolean valid(byte i) {
            return i <= 0;
        }

    }

    @FunctionEvaluator("greater than or equal to")
    public static class GreaterOrEqualTo extends ComparisonEvaluator {

        public GreaterOrEqualTo(RecordPointer record, FunctionArguments args) {
            super(record, args);
        }

        @Override
        public boolean valid(byte i) {
            return i >= 0;
        }

    }

    @FunctionEvaluator("and")
    public static class And extends BaseBasicEvaluator{
        private final BasicEvaluator left;
        private final BasicEvaluator right;

        public And( RecordPointer record, FunctionArguments args) {

            super(args.isOnlyConstants(),record);
            this.left = args.getEvaluator(0);
            this.right = args.getEvaluator(1);
        }

        @Override
        public DrillValue eval() {
            Bit leftOp = (Bit) left.eval();
            Bit rightOp = (Bit) right.eval();
            Bit value = new Bit(null, BufferAllocator.getAllocator(null)) ;
            value.allocateNew(leftOp.capacity());
            value.setRecordCount(leftOp.getRecordCount());
            for(int i = 0 ; i < leftOp.getRecordCount() ; i++){
                if(leftOp.getBit(i) == 1 && rightOp.getBit(i) == 1){
                    value.set(i);
                }
            }
            return  value;
        }
    }


}
