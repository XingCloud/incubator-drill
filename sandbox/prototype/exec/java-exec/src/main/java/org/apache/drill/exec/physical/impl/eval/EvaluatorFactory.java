package org.apache.drill.exec.physical.impl.eval;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.record.vector.ValueVector;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:45 PM
 */
public abstract class EvaluatorFactory {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EvaluatorFactory.class);

    public abstract BooleanEvaluator getBooleanEvaluator(RecordPointer record,LogicalExpression e);

    public abstract BasicEvaluator getBasicEvaluator(RecordPointer record,LogicalExpression e);

    public abstract AggregatingEvaluator getAggregateEvaluator(RecordPointer record,LogicalExpression e);




}
