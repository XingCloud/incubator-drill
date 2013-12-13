package org.apache.drill.exec.physical.impl.eval;

/*
import static org.apache.drill.common.util.DrillConstants.DOUBLE_SLASH;
import static org.apache.drill.common.util.DrillConstants.DOUBLE_SLASH_PLACEHOLDER;
 */
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AggregateChecker;
import org.apache.drill.common.expression.visitors.ConstantChecker;
import org.apache.drill.common.expression.visitors.SimpleExprVisitor;
import org.apache.drill.exec.physical.impl.eval.ConstantValues.BooleanScalar;
import org.apache.drill.exec.physical.impl.eval.ConstantValues.DoubleScalar;
import org.apache.drill.exec.physical.impl.eval.ConstantValues.LongScalar;
import org.apache.drill.exec.physical.impl.eval.ConstantValues.StringScalar;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluatorRegistry;
import org.apache.drill.exec.record.RecordBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA. User: witwolf Date: 7/7/13 Time: 3:46 PM
 */
public class SimpleEvaluatorVistor extends SimpleExprVisitor<BasicEvaluator> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleEvaluatorVistor.class);

  private RecordBatch recordBatch;

  public SimpleEvaluatorVistor(RecordBatch recordBatch) {
    super();
    this.recordBatch = recordBatch;
  }

  @Override
  public BasicEvaluator visitFunctionCall(FunctionCall call) {
    List<BasicEvaluator> evals = new ArrayList<BasicEvaluator>();
    boolean includesAggregates = false;
    boolean onlyConstants = true;
    for (LogicalExpression e : call) {
      if (AggregateChecker.isAggregating(e))
        includesAggregates = true;
      if (!ConstantChecker.onlyIncludesConstants(e))
        onlyConstants = false;
      evals.add(e.accept(this, null));
    }

    FunctionArguments args = new FunctionArguments(onlyConstants, includesAggregates, evals, call);
    return FunctionEvaluatorRegistry.getEvaluator(call.getDefinition().getName(), args, recordBatch);
  }

  @Override
  public BasicEvaluator visitIfExpression(IfExpression ifExpr) {
    return null;
  }

  @Override
  public BasicEvaluator visitSchemaPath(SchemaPath path) {
    return new FieldEvaluator(path, recordBatch);
  }

  @Override
  public BasicEvaluator visitLongConstant(ValueExpressions.LongExpression intExpr) {
    return new LongScalar(intExpr.getLong(), recordBatch);
  }

  @Override
  public BasicEvaluator visitDoubleConstant(ValueExpressions.DoubleExpression dExpr) {
    return new DoubleScalar(dExpr.getDouble(), recordBatch);
  }

  @Override
  public BasicEvaluator visitBooleanConstant(ValueExpressions.BooleanExpression e) {
    return new BooleanScalar(e.getBoolean(), recordBatch);
  }

  @Override
  public BasicEvaluator visitQuotedStringConstant(ValueExpressions.QuotedString e) {
    //return new StringScalar(e.value.replace(DOUBLE_SLASH_PLACEHOLDER, DOUBLE_SLASH), recordBatch);
    return new StringScalar(e.value,recordBatch);
  }

  @Override
  public BasicEvaluator visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return null;
  }
}
