package org.apache.drill.sql.visitors;

import static org.apache.drill.sql.AggrFunction.COUNT;
import static org.apache.drill.sql.AggrFunction.COUNT_D;
import static org.apache.drill.sql.AggrFunction.SUM;
import static org.apache.drill.sql.BinaryOperator.AND;
import static org.apache.drill.sql.BinaryOperator.EQ;
import static org.apache.drill.sql.BinaryOperator.GE;
import static org.apache.drill.sql.BinaryOperator.GT;
import static org.apache.drill.sql.BinaryOperator.LE;
import static org.apache.drill.sql.BinaryOperator.LT;
import static org.apache.drill.sql.BinaryOperator.OR;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-7-4 Time: 下午6:43 Package: org.apache.drill.sql.visitors
 */
public class SqlExpressionVisitor implements ExpressionVisitor {

  private LogicalExpression le;

  private boolean isDistinct;

  @Override public void visit(NullValue nullValue) {
  }

  @Override
  public void visit(Function function) {
    boolean isDistinct = function.isDistinct();
    this.isDistinct = isDistinct;
    String functionName = function.getName().toUpperCase();
    ExpressionList expressionList = function.getParameters();
    List<Expression> expressions = expressionList.getExpressions();

    SqlExpressionVisitor visitor;
    List<LogicalExpression> les = new ArrayList<>(expressions.size());
    for (Expression expression : expressions) {
      visitor = new SqlExpressionVisitor();
      expression.accept(visitor);
      les.add(visitor.getLogicalExpression());
    }
    FunctionRegistry functionRegistry = new FunctionRegistry(DrillConfig.create());
    if (COUNT.name().equals(functionName)) {
      if (isDistinct) {
        le = functionRegistry.createExpression(COUNT_D.name().toLowerCase(), les);
      } else {
        le = functionRegistry.createExpression(COUNT.name().toLowerCase(), les);
      }
    } else if (SUM.name().equals(functionName)) {
      le = functionRegistry.createExpression(SUM.name().toLowerCase(), les);
    }
  }

  private LogicalExpression visitBinaryExpression(BinaryExpression e, String sqlName) {
    Expression left = e.getLeftExpression();
    SqlExpressionVisitor leftVisitor = new SqlExpressionVisitor();
    left.accept(leftVisitor);

    Expression right = e.getRightExpression();
    SqlExpressionVisitor rightVisitor = new SqlExpressionVisitor();
    right.accept(rightVisitor);

    List<LogicalExpression> les = new ArrayList<>(2);
    les.add(leftVisitor.getLogicalExpression());
    les.add(rightVisitor.getLogicalExpression());

    FunctionRegistry functionRegistry = new FunctionRegistry(DrillConfig.create());

    return functionRegistry.createExpression(sqlName, les);
  }

  public LogicalExpression getLogicalExpression() {
    return le;
  }

  @Override public void visit(InverseExpression inverseExpression) {
  }

  @Override public void visit(JdbcParameter jdbcParameter) {
  }

  // field ref
  @Override
  public void visit(Column column) {
    le = new FieldReference(column.getWholeColumnName());
  }

  // Logical binary operation
  @Override
  public void visit(AndExpression andExpression) {
    le = visitBinaryExpression(andExpression, AND.getSqlName());
  }

  @Override
  public void visit(OrExpression orExpression) {
    le = visitBinaryExpression(orExpression, OR.getSqlName());
  }

  // Compare binary operation
  @Override
  public void visit(EqualsTo equalsTo) {
    le = visitBinaryExpression(equalsTo, EQ.getSqlName());
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    le = visitBinaryExpression(greaterThan, GT.getSqlName());
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    le = visitBinaryExpression(greaterThanEquals, GE.getSqlName());
  }

  @Override
  public void visit(MinorThan minorThan) {
    le = visitBinaryExpression(minorThan, LT.getSqlName());
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    le = visitBinaryExpression(minorThanEquals, LE.getSqlName());
  }

  // Value operation
  @Override
  public void visit(DoubleValue doubleValue) {
    le = ValueExpressions.getNumericExpression(doubleValue.toString());
  }

  @Override
  public void visit(LongValue longValue) {
    le = ValueExpressions.getNumericExpression(longValue.toString());
  }

  @Override
  public void visit(StringValue stringValue) {
    le = new ValueExpressions.QuotedString(stringValue.getValue());
  }

  // Date time operation
  @Override public void visit(DateValue dateValue) {
    le = new ValueExpressions.QuotedString(dateValue.getValue().toString());
  }

  @Override public void visit(TimeValue timeValue) {
  }

  @Override public void visit(TimestampValue timestampValue) {
  }

  @Override public void visit(Parenthesis parenthesis) {
  }

  @Override public void visit(Addition addition) {
  }

  @Override public void visit(Division division) {
  }

  @Override public void visit(Multiplication multiplication) {
  }

  @Override public void visit(Subtraction subtraction) {
  }

  @Override public void visit(Between between) {
  }

  @Override public void visit(InExpression inExpression) {
  }

  @Override public void visit(IsNullExpression isNullExpression) {
  }

  @Override public void visit(LikeExpression likeExpression) {
  }

  @Override public void visit(NotEqualsTo notEqualsTo) {
  }

  @Override public void visit(SubSelect subSelect) {
  }

  @Override public void visit(CaseExpression caseExpression) {
  }

  @Override public void visit(WhenClause whenClause) {
  }

  @Override public void visit(ExistsExpression existsExpression) {
  }

  @Override public void visit(AllComparisonExpression allComparisonExpression) {
  }

  @Override public void visit(AnyComparisonExpression anyComparisonExpression) {
  }

  @Override public void visit(Concat concat) {
  }

  @Override public void visit(Matches matches) {
  }

  @Override public void visit(BitwiseAnd bitwiseAnd) {
  }

  @Override public void visit(BitwiseOr bitwiseOr) {
  }

  @Override public void visit(BitwiseXor bitwiseXor) {
  }

  public LogicalExpression getLe() {
    return le;
  }

  public boolean isDistinct() {
    return this.isDistinct;
  }
}
