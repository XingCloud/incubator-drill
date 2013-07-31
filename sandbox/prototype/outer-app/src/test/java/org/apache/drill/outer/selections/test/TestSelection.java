package org.apache.drill.outer.selections.test;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.junit.Test;

/**
 * User: Z J Wu
 * Date: 13-7-30
 * Time: 下午5:44
 * Package: org.apache.drill.outer.selections.test
 */
public class TestSelection {

  @Test
  public void testSelectionBuild() {
    LogicalExpression longValueExpression = new ValueExpressions.LongExpression(1l, ExpressionPosition.UNKNOWN);
    System.out.println(longValueExpression);
  }
}
