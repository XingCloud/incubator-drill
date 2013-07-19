package org.apache.drill.common.logical.manual;

import static org.apache.drill.common.enums.Aggregator.COUNT;
import static org.apache.drill.common.enums.Aggregator.COUNT_DISTINCT;
import static org.apache.drill.common.enums.Aggregator.SUM;
import static org.apache.drill.common.enums.BinaryOperator.AND;
import static org.apache.drill.common.enums.BinaryOperator.EQ;
import static org.apache.drill.common.enums.GroupByType.USER_PROPERTY;
import static org.apache.drill.common.util.DrillConstants.HBASE_TABLE_PREFIX_EVENT;
import static org.apache.drill.common.util.DrillConstants.HBASE_TABLE_PREFIX_USER;
import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildTable;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.enums.BinaryOperator;
import org.apache.drill.common.enums.GroupByType;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Segment;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.util.Selections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-7-8 Time: 下午3:31 Package: org.apache.drill.sql.manual
 */
public class ManualStaticLPBuilder {

  public static class Grouping {
    private String groupby;

    private GroupByType groupByType;

    private String func;

    private Grouping() {
    }

    public String getGroupby() {
      return groupby;
    }

    public void setGroupby(String groupby) {
      this.groupby = groupby;
    }

    public GroupByType getGroupByType() {
      return groupByType;
    }

    public void setGroupByType(GroupByType groupByType) {
      this.groupByType = groupByType;
    }

    public String getFunc() {
      return func;
    }

    public void setFunc(String func) {
      this.func = func;
    }

    public static Grouping buildEventGroup(int level) {
      Grouping g = new Grouping();
      g.setGroupByType(GroupByType.EVENT);
      g.setGroupby("l" + level);
      return g;
    }

    public static Grouping buildUserGroup(String groupby) {
      Grouping g = new Grouping();
      g.setGroupByType(GroupByType.USER_PROPERTY);
      g.setGroupby(groupby);
      return g;
    }

    public static Grouping buildFuncGroup(String func, String groupby) {
      Grouping g = new Grouping();
      g.setGroupByType(GroupByType.INTERNAL_FUNC);
      g.setGroupby(groupby);
      g.setFunc(func);
      return g;
    }

  }

  private static PlanProperties DEFAULT_LOGICAL_PLAN_PROPERTIES;

  static {
    ObjectMapper mapper = new ObjectMapper();
    try {
      DEFAULT_LOGICAL_PLAN_PROPERTIES = mapper.readValue(new String(
        "{\"type\":\"APACHE_DRILL_LOGICAL\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}")
                                                           .getBytes(), PlanProperties.class);
    } catch (IOException e) {
      DEFAULT_LOGICAL_PLAN_PROPERTIES = null;
    }
  }

  private static class EventSlice {
    private String event;
    private int location;

    public EventSlice(String event, int location) {
      this.event = event;
      this.location = location;
    }

  }

  private static List<EventSlice> event2Array(String event) {
    String[] eventArr = event.split("\\.");
    List<EventSlice> list = new ArrayList<>(eventArr.length);
    for (int i = 0; i < eventArr.length; i++) {
      if (!"*".equals(eventArr[i])) {
        list.add(new EventSlice(eventArr[i], i));
      }
    }
    return list;
  }

  private static LogicalExpression buildEventExpression(String tableName, String event) {
    List<EventSlice> eventArr = event2Array(event);
    EventSlice es;
    LogicalExpression left, right;
    Iterator<EventSlice> it = eventArr.iterator();
    es = it.next();
    left = buildSingleLogicalExpression(tableName, "l" + es.location, es.event, EQ);
    while (it.hasNext()) {
      es = it.next();
      right = buildSingleLogicalExpression(tableName, "l" + es.location, es.event, EQ);
      left = buildBinaryLogicalExpression(left, right);
    }

    return left;
  }

  private static FunctionRegistry functionRegistry = new FunctionRegistry(DrillConfig.create());

  private static LogicalExpression buildSingleLogicalExpression(String tableName, String column, Object columnValue,
                                                                BinaryOperator operator) {
    List<LogicalExpression> lrLogicalExprList = new ArrayList<>(2);
    String wholeColumnName = tableName + "." + column;
    FieldReference fr = new FieldReference(wholeColumnName);
    lrLogicalExprList.add(fr);
    if (columnValue instanceof Number) {
      lrLogicalExprList.add(ValueExpressions.getNumericExpression(columnValue.toString()));
    } else {
      lrLogicalExprList.add(new ValueExpressions.QuotedString(columnValue.toString()));
    }
    return functionRegistry.createExpression(operator.getSqlName(), lrLogicalExprList);
  }

//  private static LogicalExpression buildBinaryLogicalExpression(LogicalExpression left, LogicalExpression right) {
//    return functionRegistry.createExpression(AND.getSqlName(), left, right);
//  }

  private static LogicalExpression buildBinaryLogicalExpression(LogicalExpression... expressions) {
    if (ArrayUtils.isEmpty(expressions)) {
      return null;
    }
    if (expressions.length == 1) {
      return expressions[0];
    }
    int counter = 0, size = expressions.length;
    LogicalExpression left, right;
    left = expressions[0];
    ++counter;

    while (counter < size) {
      right = expressions[counter];
      left = functionRegistry.createExpression(AND.getSqlName(), left, right);
      ++counter;
    }

    return left;
  }

  public static LogicalPlan buildStaticLogicalPlanManually(String projectId, String event, String date,
                                                           Map<String, Object> segmentMap, Grouping grouping) throws
    IOException {
    List<LogicalOperator> logicalOperators = new ArrayList<>();

    // Build from item
    String eventTable = projectId + HBASE_TABLE_PREFIX_EVENT;
    String userTable = projectId + HBASE_TABLE_PREFIX_USER;

    boolean hasSegment = MapUtils.isNotEmpty(segmentMap);
    boolean groupingQuery = grouping != null;

    boolean needJoin = hasSegment || (groupingQuery && USER_PROPERTY.equals(grouping.getGroupByType())
    );

    FieldReference fr = buildTable(eventTable);
    Scan eventTableScan = new Scan(SE_HBASE, Selections.buildEventSelection(projectId, date, date, event), fr);
    eventTableScan.setMemo("Scan(Table=" + eventTable + ")");
    logicalOperators.add(eventTableScan);

    Scan userTableScan = null;
    if (needJoin) {
      fr = buildTable(userTable);
      userTableScan = new Scan(SE_HBASE, Selections.buildUserSelection(projectId, grouping.getGroupby()), fr);
      userTableScan.setMemo("Scan(Table=" + userTable + ")");
      logicalOperators.add(userTableScan);
    }

    if (hasSegment) {
      for (Map.Entry<String, Object> entry : segmentMap.entrySet()) {

      }
    }

    // Build fixed selections
//    LogicalExpression condition1 = buildEventExpression(eventTable, event);
//    LogicalExpression condition2 = buildSingleLogicalExpression(eventTable, "date", date, EQ);
//    LogicalExpression combine = buildBinaryLogicalExpression(condition1, condition2);
//    Filter eventfilter = new Filter(combine);
//    eventfilter.setInput(fromEventTable);
//    logicalOperators.add(eventfilter);
//
//    Filter userFilter = null;
//    if (needJoin) {
//      LogicalExpression[] userConditions = new LogicalExpression[segmentMap.size()];
//      int counter = 0;
//      for (Map.Entry<String, Object> entry : segmentMap.entrySet()) {
//        userConditions[counter] = buildSingleLogicalExpression(userTable, entry.getKey(), entry.getValue(), EQ);
//      }
//      combine = buildBinaryLogicalExpression(userConditions);
//      userFilter = new Filter(combine);
//      userFilter.setInput(fromUserTable);
//      logicalOperators.add(userFilter);
//    }

    Join join = null;
    JoinCondition[] joinConditions;
    if (needJoin) {
      joinConditions = new JoinCondition[1];
      joinConditions[0] = new JoinCondition("==", buildColumn(eventTable, "uid"), buildColumn(userTable, "uid"));
      join = new Join(eventTableScan, userTableScan, joinConditions, Join.JoinType.INNER);
      logicalOperators.add(join);
    }

    // Build segment(Group By)
    Segment segment = null;
    String groupByRef;
    LogicalExpression singleGroupByLE;
    if (grouping != null) {
      GroupByType groupByType = grouping.getGroupByType();
      String groupBy = grouping.getGroupby();
      if (GroupByType.INTERNAL_FUNC.equals(groupByType)) {
        String func = grouping.getFunc();
        singleGroupByLE = functionRegistry.createExpression(func, new FieldReference(groupBy));
        groupByRef = eventTable + "." + grouping.getFunc() + "." + groupBy;
        segment = new Segment(new LogicalExpression[]{singleGroupByLE}, new FieldReference("segment_" + groupByRef));
      } else if (GroupByType.EVENT.equals(groupByType)) {
        groupByRef = eventTable + "." + groupBy;
        singleGroupByLE = new FieldReference(groupByRef);
        segment = new Segment(new LogicalExpression[]{singleGroupByLE}, new FieldReference("segment_" + groupByRef));
      } else {
        groupByRef = userTable + "." + groupBy;
        singleGroupByLE = new FieldReference(groupByRef);
        segment = new Segment(new LogicalExpression[]{singleGroupByLE}, new FieldReference("segment_" + groupByRef));
      }
      if (needJoin) {
        segment.setInput(join);
      } else {
        segment.setInput(eventTableScan);
      }
      logicalOperators.add(segment);
    }

    // Build collapsing aggregation
    CollapsingAggregate collapsingAggregate;
    FieldReference within = (segment == null ? null : segment.getName()), target = null;
    FieldReference[] carryovers = null;
    NamedExpression[] namedExpressions = new NamedExpression[3];

    if (groupingQuery) {
      carryovers = new FieldReference[1];
      if (grouping.getGroupByType().equals(GroupByType.EVENT)) {
        carryovers[0] = new FieldReference(grouping.getFunc() + "(" + eventTable + "." + grouping.getGroupby() + ")");
      } else if (grouping.getGroupByType().equals(GroupByType.INTERNAL_FUNC)) {
        carryovers[0] = new FieldReference(eventTable + "." + grouping.getGroupby());
      } else {
        carryovers[0] = new FieldReference(userTable + "." + grouping.getGroupby());
      }
    }

    String aggrColumn = eventTable + ".uid";
    FunctionCall fc;
    FieldReference aggrOn = new FieldReference(aggrColumn);
    fc = (FunctionCall) functionRegistry.createExpression(COUNT.getKeyWord(), aggrOn);
    namedExpressions[0] = new NamedExpression(fc, buildColumn("event_count"));

    fc = (FunctionCall) functionRegistry.createExpression(COUNT_DISTINCT.getKeyWord(), aggrOn);
    namedExpressions[1] = new NamedExpression(fc, buildColumn("user_number"));

    aggrColumn = eventTable + ".value";
    aggrOn = new FieldReference(aggrColumn);
    fc = (FunctionCall) functionRegistry.createExpression(SUM.getKeyWord(), aggrOn);
    namedExpressions[2] = new NamedExpression(fc, buildColumn("event_sum"));
    collapsingAggregate = new CollapsingAggregate(within, target, carryovers, namedExpressions);

    if (groupingQuery) {
      collapsingAggregate.setInput(segment);
    } else {
      if (needJoin) {
        collapsingAggregate.setInput(join);
      } else {
        collapsingAggregate.setInput(eventTableScan);
      }
    }

    logicalOperators.add(collapsingAggregate);

    // Output
    Store store = getStore();
    store.setInput(collapsingAggregate);
    logicalOperators.add(store);

//    ObjectMapper mapper = new ObjectMapper();
    Map<String, StorageEngineConfig> storageEngineMap = new HashMap<>();
//    storageEngineMap.put("console", mapper
//      .readValue(new String("{\"type\":\"console\"}").getBytes(), ConsoleRSE.ConsoleRSEConfig.class));
    LogicalPlan logicalPlan = new LogicalPlan(DEFAULT_LOGICAL_PLAN_PROPERTIES, storageEngineMap, logicalOperators);
    return logicalPlan;
  }

  private static Store getStore() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return new Store("console",
                       mapper.readValue(new String("{\"file\":\"console:///stdout\"}").getBytes(), JSONOptions.class),
                       null);
    } catch (Exception e) {
      return null;
    }
  }

  public static void main(String[] args) throws IOException {
    DrillConfig c = DrillConfig.create();
    LogicalPlan logicalPlan;
//    logicalPlan = buildStaticLogicalPlanManually("sof_dsk", "a.b.c.*", "20130708", null);
//    System.out.println(logicalPlan.unparse(c));

    Map<String, Object> segmentMap = new HashMap<>(1);
    segmentMap.put("register_time", "2013-07-12");
    System.out.println("---------------------------------");
    logicalPlan = buildStaticLogicalPlanManually("ddt", "visit.*", "20130701", null,
                                                 Grouping.buildUserGroup("language"));
    System.out.println(logicalPlan.toJsonString(c));
  }
}
