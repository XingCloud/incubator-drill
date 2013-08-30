package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.SimpleExprVisitor;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BitVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:46 PM
 */
public class BasicEvaluatorFactory extends EvaluatorFactory {
    static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicEvaluatorFactory.class);

    public BooleanEvaluator getBooleanEvaluator(RecordBatch recordBatch, LogicalExpression e) {
        return new BooleanEvaluatorImp(e.accept(get(recordBatch), null));
    }

    @Override
    public BasicEvaluator getBasicEvaluator(RecordBatch recordBatch, LogicalExpression e) {
        return e.accept(get(recordBatch), null);
    }

    @Override
    public AggregatingEvaluator getAggregateEvaluator(RecordBatch recordBatch, LogicalExpression e) {
        SimpleEvaluatorVistor vistor = new SimpleEvaluatorVistor(recordBatch);
        return (AggregatingEvaluator) e.accept(vistor, null);
    }


    private SimpleExprVisitor<BasicEvaluator> get(RecordBatch recordBatch) {
        return new SimpleEvaluatorVistor(recordBatch);
    }

    private class BooleanEvaluatorImp implements BooleanEvaluator {
        private BasicEvaluator evaluator;

        private BooleanEvaluatorImp(BasicEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public BitVector eval() {
            return (BitVector) evaluator.eval();
        }
    }

}
