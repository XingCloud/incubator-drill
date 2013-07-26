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
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-7-4 Time: 下午3:54 Package: org.apache.drill.sql.test
 */
public class TestSelect {
  private static CCJSqlParserManager PARSER = new CCJSqlParserManager();

  public static void testSimpleSelect() throws JSQLParserException {
    String sql;
    sql = "SELECT count(age_deu.uid) from ( age_deu inner join age_user on age_user.uid=age_deu.uid ) group by hour(age_user.nation)";
    Statement statement = PARSER.parse(new StringReader(sql));
    StatementDeParser deParser = new StatementDeParser(new StringBuffer());
    statement.accept(deParser);
    Expression expression;

    Select select = (Select) statement;
    PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
    SubJoin sj = (SubJoin) plainSelect.getFromItem();

    FunctionRegistry functionRegistry = new FunctionRegistry(DrillConfig.create());
    List<LogicalExpression> args = new ArrayList<LogicalExpression>();

    BinaryExpression be = (BinaryExpression) sj.getJoin().getOnExpression();
    String wholeColumnName = ((Column) be.getLeftExpression()).getWholeColumnName();
    System.out.println(wholeColumnName);

    FieldReference fr = new FieldReference("a.b");
    System.out.println(fr);
    args.add(new FieldReference(((Column) be.getLeftExpression()).getWholeColumnName()));
    args.add(new FieldReference(((Column) be.getRightExpression()).getWholeColumnName()));
    FunctionCall le = (FunctionCall) functionRegistry.createExpression("==", args);

    SelectExpressionItem sei;
    List<Object> selectItems = plainSelect.getSelectItems();
    for (Object o : selectItems) {
      sei = (SelectExpressionItem) o;
      System.out.println(o.getClass());
    }

    List<Object> groupByItems = plainSelect.getGroupByColumnReferences();

    for (Object o : groupByItems) {
      System.out.println(o.getClass());
    }
    System.out.println("---------------------------------");
    Function f;
    ExpressionList eList;
    for (Object o : plainSelect.getSelectItems()) {
      sei = (SelectExpressionItem) o;
      System.out.println(sei);
    }
  }

  public static void main(String[] args) throws JSQLParserException {
    testSimpleSelect();
  }
}