package org.apache.drill.common.expression;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;

/**
 * User: Z J Wu
 * Date: 13-7-31
 * Time: 上午10:24
 * Package: org.apache.drill.common.expression
 */
public class LogicalExpressionParser {
  public static LogicalExpression parse(String expr) throws RecognitionException {
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    parser.setRegistry(new FunctionRegistry(DrillConfig.create()));
    ExprParser.parse_return ret = parser.parse();
    return ret.e;
  }

  public static void main(String[] args) throws RecognitionException {
    LogicalExpression le = LogicalExpressionParser.parse("a+1");
    System.out.println(le);
  }
}
