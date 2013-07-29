package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/5/13
 * Time: 7:21 PM
 */
public class MathEvaluators {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathEvaluators.class);

    @FunctionEvaluator("add")
    public static class AddEvaluator extends BaseBasicEvaluator {
        private final BasicEvaluator[] args;

        public AddEvaluator(RecordBatch recordBatch, FunctionArguments args) {
            super(args.isOnlyConstants(), recordBatch);
            this.args = args.getArgsAsArray();
        }

        @Override
        public ValueVector eval() {
            ValueVector dv = args[0].eval();
            for (int i = 1; i < args.length; i++) {
                // TODO
            }
            return dv;
        }
    }

    @FunctionEvaluator("multiply")
    public static class MultiplyE extends BaseBasicEvaluator {
        private final BasicEvaluator args[];

        public MultiplyE(RecordBatch recordBatch, FunctionArguments args) {
            super(args.isOnlyConstants(), recordBatch);
            this.args = args.getArgsAsArray();
        }

        @Override
        public ValueVector eval() {
            // TODO
            return null;
        }
    }
}
