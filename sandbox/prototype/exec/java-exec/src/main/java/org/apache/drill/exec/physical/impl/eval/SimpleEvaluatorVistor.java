package org.apache.drill.exec.physical.impl.eval;

import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.visitors.AggregateChecker;
import org.apache.drill.common.expression.visitors.ConstantChecker;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluatorRegistry;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.record.values.ScalarValues;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:46 PM
 */
public class SimpleEvaluatorVistor implements ExprVisitor<BasicEvaluator> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleEvaluatorVistor.class);

    private RecordPointer record;

    private List<AggregatingEvaluator> aggregators = new ArrayList<>();

    public SimpleEvaluatorVistor(RecordPointer record) {
        super();
        this.record = record;
    }

    public List<AggregatingEvaluator> getAggregators() {
        return aggregators;
    }

    @Override
    public BasicEvaluator visitFunctionCall(FunctionCall call) {
        List<BasicEvaluator> evals = new ArrayList<BasicEvaluator>();
        boolean includesAggregates = false;
        boolean onlyConstants = true;
        for (LogicalExpression e : call) {
            if(AggregateChecker.isAggregating(e)) includesAggregates = true;
            if(!ConstantChecker.onlyIncludesConstants(e)) onlyConstants = false;
            evals.add(e.accept(this));
        }

        FunctionArguments args = new FunctionArguments(onlyConstants, includesAggregates, evals, call);
        if (call.getDefinition().isAggregating()) {
            BasicEvaluator e = FunctionEvaluatorRegistry.getEvaluator(call.getDefinition().getName(),args,record);
            aggregators.add((AggregatingEvaluator) e);
            return e;
        } else {
            BasicEvaluator eval = FunctionEvaluatorRegistry.getEvaluator(call.getDefinition().getName(), args, record);
            return eval;

        }
    }

    @Override
    public BasicEvaluator visitIfExpression(IfExpression ifExpr) {
        return null;
    }

    @Override
    public BasicEvaluator visitSchemaPath(SchemaPath path) {
        return new FieldEvaluator(path, record);
    }

    @Override
    public BasicEvaluator visitLongExpression(ValueExpressions.LongExpression intExpr) {
        return new ScalarValues.LongScalar(intExpr.getLong());
    }

    @Override
    public BasicEvaluator visitDoubleExpression(ValueExpressions.DoubleExpression dExpr) {
        return new ScalarValues.DoubleScalar(dExpr.getDouble());
    }

    @Override
    public BasicEvaluator visitBoolean(ValueExpressions.BooleanExpression e) {
        return new ScalarValues.BooleanScalar(e.getBoolean());
    }

    @Override
    public BasicEvaluator visitQuotedString(ValueExpressions.QuotedString e) {
        return new ScalarValues.StringScalar(e.value);
    }
}
