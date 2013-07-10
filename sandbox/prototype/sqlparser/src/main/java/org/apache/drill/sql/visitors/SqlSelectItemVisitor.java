package org.apache.drill.sql.visitors;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

/**
 * User: Z J Wu Date: 13-7-5 Time: 下午3:04 Package: org.apache.drill.sql.visitors
 */
public class SqlSelectItemVisitor implements SelectItemVisitor {

  private LogicalExpression logicalExpression;
  private boolean isDistinct;

  @Override
  public void visit(AllColumns allColumns) {
    logicalExpression = new FieldReference(allColumns.toString());
  }

  @Override
  public void visit(AllTableColumns allTableColumns) {
    logicalExpression = new FieldReference(allTableColumns.toString());
  }

  @Override
  public void visit(SelectExpressionItem selectExpressionItem) {
    Expression expression = selectExpressionItem.getExpression();
    SqlExpressionVisitor expressionVisitor = new SqlExpressionVisitor();
    expression.accept(expressionVisitor);
    isDistinct = expressionVisitor.isDistinct();
    this.logicalExpression = expressionVisitor.getLogicalExpression();
  }

  public LogicalExpression getLogicalExpression() {
    return logicalExpression;
  }

  public boolean isDistinct() {
    return isDistinct;
  }
}
