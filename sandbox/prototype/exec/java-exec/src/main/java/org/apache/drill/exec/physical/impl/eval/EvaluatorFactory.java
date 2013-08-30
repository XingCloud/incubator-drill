package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:45 PM
 */
public abstract class EvaluatorFactory {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EvaluatorFactory.class);

    public abstract BooleanEvaluator getBooleanEvaluator(RecordBatch recordBatch, LogicalExpression e);

    public abstract BasicEvaluator getBasicEvaluator(RecordBatch recordBatch, LogicalExpression e);

    public abstract AggregatingEvaluator getAggregateEvaluator(RecordBatch recordBatch, LogicalExpression e);


}
