package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.RecordPointer;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/5/13
 * Time: 7:21 PM
 */
public class MathEvaluators {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathEvaluators.class);

    @FunctionEvaluator("add")
    public static class AddEvaluator extends BaseBasicEvaluator{
        private final BasicEvaluator[] args;

        public AddEvaluator(RecordPointer record, FunctionArguments args) {
            super(args.isOnlyConstants(), record);
            this.args = args.getArgsAsArray();
        }

        @Override
        public DrillValue eval() {
            DrillValue dv  = args[0].eval() ;
            for(int i = 1 ; i < args.length ; i++){
                // TODO
            }
            return dv;
        }
    }

    @FunctionEvaluator("multiply")
    public static class MultiplyE extends BaseBasicEvaluator{
        private final BasicEvaluator args[];

        public MultiplyE(RecordPointer record, FunctionArguments args){
            super(args.isOnlyConstants(), record);
            this.args = args.getArgsAsArray();
        }

        @Override
        public DrillValue eval() {
            // TODO
            return null;
        }
    }
}
