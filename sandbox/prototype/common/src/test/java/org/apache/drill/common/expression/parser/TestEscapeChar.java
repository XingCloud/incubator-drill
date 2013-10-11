package org.apache.drill.common.expression.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.junit.Test;

import java.io.IOException;

/**
 * User: Z J Wu Date: 13-10-11 Time: 上午10:05 Package: org.apache.drill.common.expression.parser
 */
public class TestEscapeChar {

  @Test
  public void test() throws IOException {
    ObjectMapper mapper= DrillConfig.create().getMapper();
    String str = "COMMON,ram,2013-10-01,2013-10-02,visit.*,{\"identifier\":\"\\\"null\\\"\"},VF-ALL-0-0,PERIOD";
    System.out.println(str);
    System.out.println(str);
    LogicalExpression qs = new ValueExpressions.QuotedString(str, ExpressionPosition.UNKNOWN);
    String str2 = mapper.writeValueAsString(qs);
    System.out.println(str2);
    LogicalExpression le=mapper.readValue(str2, LogicalExpression.class);
    System.out.println(((ValueExpressions.QuotedString)le).value);

  }
}
