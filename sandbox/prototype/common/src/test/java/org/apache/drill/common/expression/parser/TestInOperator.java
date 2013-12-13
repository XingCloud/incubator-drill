package org.apache.drill.common.expression.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.util.FieldReferenceBuilder;
import org.junit.Test;

/**
 * User: Z J Wu Date: 13-11-5 Time: 上午10:47 Package: org.apache.drill.common.expression.parser
 */
public class TestInOperator {

  @Test
  public void test() throws JsonProcessingException {
    DrillConfig config = DrillConfig.create();
    final FunctionRegistry DFR = new FunctionRegistry(config);
    LogicalExpression le = DFR
      .createExpression("in", ExpressionPosition.UNKNOWN, FieldReferenceBuilder.buildColumn("a"),
                        new ValueExpressions.QuotedString("b','c','d", ExpressionPosition.UNKNOWN));
    System.out.println(config.getMapper().writeValueAsString(le));

    le = DFR.createExpression("in", ExpressionPosition.UNKNOWN, FieldReferenceBuilder.buildColumn("a"),
                              FieldReferenceBuilder.buildColumn("1,2,3"));
    System.out.println(config.getMapper().writeValueAsString(le));
  }
}
