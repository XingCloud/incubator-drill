package org.apache.drill.sql.visitors;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Store;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Z J Wu Date: 13-7-5 Time: 上午10:56 Package: org.apache.drill.sql.visitors
 */
public class SqlQueryVisitor implements SelectVisitor {
  private List<LogicalOperator> logicalOperators = new ArrayList<>();

  public List<LogicalOperator> getLogicalOperators() {
    return logicalOperators;
  }

  @Override
  public void visit(PlainSelect plainSelect) {
    // Scan
    FromItem item = plainSelect.getFromItem();
    SqlFromVisitor fromVisitor = new SqlFromVisitor();
    item.accept(fromVisitor);
    LogicalOperator from = fromVisitor.getLogicalOperator();
    logicalOperators.add(from);

    // Selection
    Expression where = plainSelect.getWhere();
    Filter filter = null;
    LogicalExpression logicalExpression;
    if (where != null) {
      SqlExpressionVisitor exprVisitor = new SqlExpressionVisitor();
      where.accept(exprVisitor);
      logicalExpression = exprVisitor.getLogicalExpression();
      filter = new Filter(logicalExpression);
      filter.setInput(from);
      logicalOperators.add(filter);
    }

    // Distinction
    List<SelectItem> selectItems = plainSelect.getSelectItems();
    Distinct distinct = null;
    List<LogicalExpression> selectItemlogicalExpressions = new ArrayList<LogicalExpression>(selectItems.size());
    FieldReference within;
    SqlSelectItemVisitor selectItemVisitor;
    for (SelectItem selectItem : selectItems) {
      selectItemVisitor = new SqlSelectItemVisitor();
      selectItem.accept(selectItemVisitor);
      logicalExpression = selectItemVisitor.getLogicalExpression();
      selectItemlogicalExpressions.add(logicalExpression);

    }
    // Collapsing aggregation
    NamedExpression[] selections = changeToNamedExpressions(selectItemlogicalExpressions);
    CollapsingAggregate collapsingAggregate = getCollapsingAggregate(selections);
    if (collapsingAggregate != null) {
      if (filter != null) {
        collapsingAggregate.setInput(filter);
      } else {
        collapsingAggregate.setInput(from);
      }
      logicalOperators.add(collapsingAggregate);
    }

    // Projection

    // Output
    Store store = getStore();
    store.setInput(collapsingAggregate);
    logicalOperators.add(store);
  }

  @Override
  // TODO
  public void visit(Union union) {
  }

  private NamedExpression[] changeToNamedExpressions(List<LogicalExpression> logicalExpressions) {
    NamedExpression[] namedExpressions = new NamedExpression[logicalExpressions.size()];
    int cnt = 0;
    NamedExpression namedExpression;
    for (LogicalExpression exprTmp : logicalExpressions) {
      if (exprTmp instanceof FieldReference) {
        namedExpression = new NamedExpression(exprTmp, (FieldReference) exprTmp);
      } else if (exprTmp instanceof FunctionCall) {
        LogicalExpression ref = ((FunctionCall) exprTmp).args.get(0);
        String functionName = ((FunctionCall) exprTmp).getDefinition().getName();
        FieldReference newFieldRef;
        if (ref instanceof SchemaPath) {
          newFieldRef = new FieldReference(functionName + "." + ((SchemaPath) ref).getPath());
        } else {
          newFieldRef = new FieldReference(functionName + "." + ((ValueExpressions.LongExpression) ref).getLong());
        }
        namedExpression = new NamedExpression(exprTmp, newFieldRef);
      } else {
        namedExpression = null;
      }
      namedExpressions[cnt] = namedExpression;
      ++cnt;
    }
    return namedExpressions;
  }

  private CollapsingAggregate getCollapsingAggregate(NamedExpression[] namedExpressions) {
    FieldReference target = null;
    List<FieldReference> carryovers = new ArrayList<>();
    carryovers.add(new FieldReference("segmentvalue"));
    List<NamedExpression> neList = new ArrayList<>();

    String definition;
    for (NamedExpression namedExpression : namedExpressions) {
      LogicalExpression expr = namedExpression.getExpr();
      if (expr instanceof FunctionCall) {
        definition = ((FunctionCall) expr).getDefinition().getName();
        if (definition.equals("count") || definition.equals("sum") || definition.equals("countDistinct")) {
          neList.add(namedExpression);
        }
      } else {
        carryovers.add(namedExpression.getRef());
      }
    }
    if (CollectionUtils.isNotEmpty(neList)) {
      return new CollapsingAggregate(null, target, carryovers.toArray(new FieldReference[carryovers.size()]),
                                     neList.toArray(new NamedExpression[neList.size()]));
    }

    return null;
  }

  private Store getStore() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return new Store("cache", mapper.readValue(new String("{\"cache\":\"key\"}").getBytes(), JSONOptions.class),
                       null);
    } catch (Exception e) {
      return null;
    }
  }
}
