package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.vector.Bit;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:46 PM
 */
public class BasicEvaluatorFactory extends EvaluatorFactory {
    static  org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicEvaluatorFactory.class);

    public BooleanEvaluator getBooleanEvaluator(RecordPointer record, LogicalExpression e) {
           return new BooleanEvaluatorImp(e.accept(get(record)));
    }

    @Override
    public BasicEvaluator getBasicEvaluator(RecordPointer record, LogicalExpression e) {
        return e.accept(get(record));
    }

    @Override
    public AggregatingEvaluator getAggregateEvaluator(RecordPointer record, LogicalExpression e) {
        return null;
    }



    private ExprVisitor<BasicEvaluator>  get(RecordPointer record){
        return new SimpleEvaluatorVistor(record);
    }

    private class BooleanEvaluatorImp implements BooleanEvaluator{
        private BasicEvaluator evaluator;

        private BooleanEvaluatorImp(BasicEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        @Override
        public Bit eval() {
            return  (Bit) evaluator.eval();
        }
    }

}
