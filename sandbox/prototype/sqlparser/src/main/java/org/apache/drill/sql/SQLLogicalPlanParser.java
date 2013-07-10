package org.apache.drill.sql;

import static org.apache.drill.sql.utils.SqlParseUtils.isNotSelectStatement;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.sql.utils.SqlParseUtils;
import org.apache.drill.sql.visitors.SqlQueryVisitor;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-7-4 Time: 下午5:22 Package: org.apache.drill.sql
 */
public class SqlLogicalPlanParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlLogicalPlanParser.class);

  private static final CCJSqlParserManager PARSER = new CCJSqlParserManager();

  public static LogicalPlan parse(String sql) throws SQLException, IOException {
    if (StringUtils.isBlank(sql)) {
      throw new SQLException("Empty sql.");
    }
    Statement statement;
    try {
      statement = PARSER.parse(new StringReader(sql));
    } catch (JSQLParserException e) {
      throw new SQLException(e);
    }
    if (isNotSelectStatement(statement)) {
      throw new SQLException("Unsupported sql operation - " + statement.getClass().getSimpleName());
    }
    SqlQueryVisitor queryVisitor = new SqlQueryVisitor();
    SelectBody selectBody = ((Select) statement).getSelectBody();
    selectBody.accept(queryVisitor);
    List<LogicalOperator> operators = queryVisitor.getLogicalOperators();

    ObjectMapper mapper = new ObjectMapper();
    PlanProperties head = mapper.readValue(new String(
      "{\"type\":\"apache_drill_logical_plan\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}")
                                             .getBytes(), PlanProperties.class);
    Map<String, StorageEngineConfig> storageEngines = SqlParseUtils.getStorageEngineMap();
    LogicalPlan lp = new LogicalPlan(head, storageEngines, operators);
    return lp;
  }

  public static void main(String[] args) throws SQLException, IOException {
    String simpleSelect;
    LogicalPlan lp;
    simpleSelect = "SELECT * from sof_dsk_deu";
    lp = parse(simpleSelect);

//    simpleSelect = "SELECT count( distinct sof_dsk_deu.uid) AS cnt from sof_dsk_deu";
//    lp = parse(simpleSelect);
//
//    simpleSelect = "SELECT count( distinct sof_dsk_deu.uid) AS cnt from sof_dsk_deu WHERE sof_dsk_deu.l0 = 'visit'";
//    lp = parse(simpleSelect);
//
//    simpleSelect = "SELECT count( distinct sof_dsk_deu.uid) AS cnt from sof_dsk_deu WHERE sof_dsk_deu.l0 = 'visit' AND sof_dsk_deu.date = '20130225'";
//    lp = parse(simpleSelect);
  }
}
