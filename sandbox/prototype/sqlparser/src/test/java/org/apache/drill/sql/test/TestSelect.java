package org.apache.drill.sql.test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.io.StringReader;

/**
 * User: Z J Wu Date: 13-7-4 Time: 下午3:54 Package: org.apache.drill.sql.test
 */
public class TestSelect {
  private static CCJSqlParserManager PARSER = new CCJSqlParserManager();

  public static void testSimpleSelect() throws JSQLParserException {
    String sql;
    sql = "SELECT count( distinct `sof-dsk_deu`.uid) AS cnt from `sof-dsk_deu` WHERE `sof-dsk_deu`.l0 = 'visit' AND `sof-dsk_deu`.date = '20130225'";
    sql = "SELECT count( distinct `sof-dsk_deu`.uid) AS cnt from `sof-dsk_deu`";
    Statement statement = PARSER.parse(new StringReader(sql));
    StatementDeParser deParser = new StatementDeParser(new StringBuffer());
    statement.accept(deParser);
    Expression expression;

    Select select = (Select) statement;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

    System.out.println("---------------------------------");
    SelectExpressionItem sei;
    Function f;
    ExpressionList eList;
    for (Object o : plainSelect.getSelectItems()) {
      sei = (SelectExpressionItem) o;
      f = (Function) sei.getExpression();
      eList = f.getParameters();
      System.out.println(eList.getExpressions().get(0).getClass());
    }
  }

  public static void main(String[] args) throws JSQLParserException {
    testSimpleSelect();
  }
}
