package org.apache.drill.sql.oldref;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA. User: Wang Yufei Date: 13-2-20 Time: 上午11:20 To change this template use File | Settings
 * | File Templates.
 */
public class PlanParser {

  static final Logger logger = LoggerFactory.getLogger(PlanParser.class);
  private CCJSqlParserManager pm = new CCJSqlParserManager();
  private static PlanParser instance = new PlanParser();
  private List<String> selections = null;

  private PlanParser() {

  }

  public static PlanParser getInstance() {
    return instance;
  }

  public List<String> getSelections() {
    return selections;
  }

  public LogicalPlan parse(String sql) throws JSQLParserException, Exception {
    sql = sql.replace("-", "xadrill");
    LogicalPlan plan = null;
    Statement statement = pm.parse(new StringReader(sql));
    if (statement instanceof Select) {
      Select selectStatement = (Select) statement;
      AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
      selectStatement.getSelectBody().accept(visitor);
      List<LogicalOperator> logicalOperators = visitor.getLogicalOperators();
      selections = visitor.getSelections();

      ObjectMapper mapper = new ObjectMapper();
      PlanProperties head = mapper.readValue(new String(
        "{\"type\":\"APACHE_DRILL_LOGICAL\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}")
                                               .getBytes(), PlanProperties.class);

      Map<String, StorageEngineConfig> map = new HashMap<>(1);
      plan = new LogicalPlan(head, map, logicalOperators);
    }
    return plan;
  }

  public static void test() {
    SchemaPath schemaPath = new SchemaPath("sof-dsk_deu");

  }

  public static void main(String[] args) throws Exception {
    String sql;
    sql = "select ddt_user.ref, count(ddt_deu.uid), sum(ddt_deu.value) " +
      "from (ddt_deu inner join ddt_user on ddt_deu.uid = ddt_user.uid) " +
      "where ddt_deu.date = '20130716' and ddt_deu.event = 'visit.*' group by ddt_user.ref";
    sql = "select hour(ddt_deu.timestamp), count(ddt_deu.uid), sum(ddt_deu.value) from ddt_deu where ddt_deu.date = '20130716' and ddt_deu.event = 'visit.*' group by hour(ddt_deu.timestamp)";
    LogicalPlan lp = new PlanParser().parse(sql);
    System.out.println(lp.toJsonString(DrillConfig.create()));
  }
}
