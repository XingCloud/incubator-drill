package org.apache.drill.sql.visitors;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.sql.utils.Selections;
import org.apache.drill.sql.utils.SqlParseConst;
import org.apache.drill.sql.utils.SqlParseUtils;

/**
 * User: Z J Wu Date: 13-7-5 Time: 上午11:07 Package: org.apache.drill.sql.visitors
 */
public class SqlFromVisitor implements FromItemVisitor {
  private LogicalOperator logicalOperator;

  @Override
  public void visit(Table table) {
    String tableName = table.getName();
    tableName = SqlParseUtils.sqlQuotaRemover(tableName);
    FieldReference fr = new FieldReference(tableName);
    logicalOperator = new Scan(SqlParseConst.SE_HBASE, Selections.getNoneSelection(), fr);
  }

  @Override
  // TODO
  public void visit(SubSelect subSelect) {
  }

  @Override
  public void visit(SubJoin subJoin) {
  }

  public LogicalOperator getLogicalOperator() {
    return logicalOperator;
  }
}
