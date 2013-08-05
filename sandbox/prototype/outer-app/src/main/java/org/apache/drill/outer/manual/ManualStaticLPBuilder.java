package org.apache.drill.outer.manual;

import static org.apache.drill.common.enums.Aggregator.*;
import static org.apache.drill.common.enums.BinaryOperator.AND;
import static org.apache.drill.common.enums.BinaryOperator.EQ;
import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildTable;
import static org.apache.drill.outer.utils.GenericUtils.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.enums.BinaryOperator;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.outer.enums.GroupByType;
import org.apache.drill.outer.selections.Selection;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: Z J Wu Date: 13-7-8 Time: 下午3:31 Package: org.apache.drill.sql.manual
 */
public class ManualStaticLPBuilder {

  public static class Grouping {
    private String groupby;

    private org.apache.drill.outer.enums.GroupByType groupByType;

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
      g.setGroupby("event" + level);
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


  public static class EventSlice {
    private String event;
    private int location;

    public EventSlice(String event, int location) {
      this.event = event;
      this.location = location;
    }

    public String getEvent() {
      return event;
    }

    public int getLocation() {
      return location;
    }
  }

  private static List<EventSlice> event2List(String event) {
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
    List<EventSlice> eventArr = event2List(event);
    EventSlice es;
    LogicalExpression left, right;
    Iterator<EventSlice> it = eventArr.iterator();
    es = it.next();
    left = buildSingleLogicalExpression(tableName, "event" + es.location, es.event, EQ);
    while (it.hasNext()) {
      es = it.next();
      right = buildSingleLogicalExpression(tableName, "event" + es.location, es.event, EQ);
      left = buildBinaryLogicalExpression(left, right);
    }

    return left;
  }

  private static FunctionRegistry functionRegistry = new FunctionRegistry(DrillConfig.create());

  private static LogicalExpression buildSingleLogicalExpression(String tableName, String column, Object columnValue,
                                                                BinaryOperator operator) {
    List<LogicalExpression> lrLogicalExprList = new ArrayList<>(2);
    String wholeColumnName = tableName + "." + column;
    FieldReference fr = new FieldReference(wholeColumnName, null);
    lrLogicalExprList.add(fr);
    if (columnValue instanceof Number) {
      lrLogicalExprList.add(ValueExpressions.getNumericExpression(columnValue.toString(), null));
    } else {
      lrLogicalExprList.add(new ValueExpressions.QuotedString(columnValue.toString(), null));
    }
    return functionRegistry.createExpression(operator.getSqlName(), null, lrLogicalExprList);
  }

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
      left = functionRegistry.createExpression(AND.getSqlName(), null, left, right);
      ++counter;
    }

    return left;
  }

  private static Scan buildSingleSegmentScan(String projectId, String userTable, String dateString, String propertyName, Object propVal) throws Exception {
    Map<String, Selection.KeyPartParameter> parameterMap = new HashMap<>(2);
    parameterMap.put("propnumber", Selection.KeyPartParameter.buildSingleKey(toBytes(propertyString2TinyInt(projectId, propertyName))));
    parameterMap.put("date", Selection.KeyPartParameter.buildSingleKey(toBytes(dateString)));
    if (propVal instanceof String) {
      parameterMap.put("value", Selection.KeyPartParameter.buildSingleKey(toBytes(propVal.toString())));
    } else {
      parameterMap.put("value", Selection.KeyPartParameter.buildSingleKey(toBytes((long) propVal)));
    }
    Selection.RowkeyRange rowkeyRange = Selection.resolveRowkeyRange(userTable, parameterMap);
    NamedExpression[] projections = new NamedExpression[1];
    projections[0] = new NamedExpression(new FieldReference("uid", ExpressionPosition.UNKNOWN), new FieldReference("uid", ExpressionPosition.UNKNOWN));
    Selection selection = new Selection(userTable, rowkeyRange, null, projections);
    Scan scan = new Scan(SE_HBASE, selection.toSingleJsonOptions(), new FieldReference("user_index_age", ExpressionPosition.UNKNOWN));
    scan.setMemo("Scan(Table=" + userTable + ", Prop=" + propertyName + ", Val=" + propVal + ")");
    return scan;
  }

  private static LogicalOperator buildSegment(String projectId, String userTable, String dateString, List<LogicalOperator> operators, Map<String, Object> segmentMap) throws Exception {
    Set<Map.Entry<String, Object>> entrySet = segmentMap.entrySet();
    Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
    Map.Entry<String, Object> entry = it.next();
    String propertyName = entry.getKey();
    Object propertyValue = entry.getValue();
    LogicalOperator lo1 = buildSingleSegmentScan(projectId, userTable, dateString, propertyName, propertyValue), lo2;
    operators.add(lo1);

    Join join;
    JoinCondition[] joinConditions;

    if (it.hasNext()) {
      for (; ; ) {
        if (it.hasNext()) {
          entry = it.next();
          propertyName = entry.getKey();
          propertyValue = entry.getValue();
          lo2 = buildSingleSegmentScan(projectId, userTable, dateString, propertyName, propertyValue);
          operators.add(lo2);

          joinConditions = new JoinCondition[1];
          joinConditions[0] = new JoinCondition("==", buildColumn(userTable, "uid"), buildColumn(userTable, "uid"));
          join = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
          operators.add(join);
          lo1 = join;
        } else {
          break;
        }
      }
    }
    return lo1;
  }

  private static JSONOptions toJsonOptions(DrillConfig config, Object o) throws IOException {
    ObjectMapper mapper = config.getMapper();
    return mapper.readValue(mapper.writeValueAsString(o), JSONOptions.class);
  }

  public static LogicalPlan buildStaticLogicalPlanManually(DrillConfig config, String projectId, String event, String date,
                                                           Map<String, Object> segmentMap, Grouping grouping) throws
    Exception {
    List<LogicalOperator> logicalOperators = new ArrayList<>();

    // Build from item
    String eventTable = "deu_" + projectId;
    String userTable = "user_index_" + projectId;

    boolean hasSegment = MapUtils.isNotEmpty(segmentMap);
    boolean groupingQuery = grouping != null;
    boolean userPropertyGroupingQuery = groupingQuery && grouping.getGroupByType().equals(GroupByType.USER_PROPERTY);

    FieldReference fr = buildTable(eventTable);
    Map<String, Selection.KeyPartParameter> parameterMap = new HashMap<>(2);
    parameterMap.put("date", Selection.KeyPartParameter.buildSingleKey(toBytes(date)));
    List<EventSlice> eventSlices = event2List(event);
    for (EventSlice es : eventSlices) {
      parameterMap.put("event" + es.getLocation(), Selection.KeyPartParameter.buildSingleKey(toBytes(es.getEvent())));
    }

    Selection.RowkeyRange rowkeyRange = Selection.resolveRowkeyRange(eventTable, parameterMap);
    LogicalExpression[] filters = null;
    NamedExpression[] projections = new NamedExpression[2];
    projections[0] = new NamedExpression(new FieldReference("uid", ExpressionPosition.UNKNOWN), new FieldReference("uid", ExpressionPosition.UNKNOWN));
    projections[1] = new NamedExpression(new FieldReference("value", ExpressionPosition.UNKNOWN), new FieldReference("value", ExpressionPosition.UNKNOWN));

    Selection selection = new Selection(eventTable, rowkeyRange, filters, projections);
    List<Map<String, Object>> mapList = new ArrayList<>(1);
    mapList.add(selection.toSelectionMap());

    Scan userTableScan, eventTableScan = new Scan(SE_HBASE, toJsonOptions(config, mapList), fr);
    eventTableScan.setMemo("Scan(Table=" + eventTable + ")");
    logicalOperators.add(eventTableScan);

    Join join;
    JoinCondition[] joinConditions;
    String dateString = new SimpleDateFormat("yyyyMMdd").format(new Date());
    // common query
    LogicalOperator scanRoot, segmentLogicalOperator;
    if (!userPropertyGroupingQuery) {
      if (hasSegment) {
        segmentLogicalOperator = buildSegment(projectId, userTable, dateString, logicalOperators, segmentMap);
        joinConditions = new JoinCondition[1];
        joinConditions[0] = new JoinCondition("==", buildColumn(eventTable, "uid"), buildColumn(userTable, "uid"));
        join = new Join(eventTableScan, segmentLogicalOperator, joinConditions, Join.JoinType.INNER);
        logicalOperators.add(join);
        scanRoot = join;
      } else {
        scanRoot = eventTableScan;
      }
    }
    // group by query
    else {
      short propShort = propertyString2TinyInt(projectId, grouping.getGroupby());
      parameterMap = new HashMap<>(1);
      parameterMap.put("propnumber", Selection.KeyPartParameter.buildSingleKey(toBytes(propShort)));
      parameterMap.put("date", Selection.KeyPartParameter.buildSingleKey(toBytes(dateString)));
      rowkeyRange = Selection.resolveRowkeyRange(userTable, parameterMap);
      projections = new NamedExpression[2];
      projections[0] = new NamedExpression(new FieldReference("uid", ExpressionPosition.UNKNOWN), new FieldReference("uid", ExpressionPosition.UNKNOWN));
      projections[1] = new NamedExpression(new FieldReference(grouping.getGroupby(), ExpressionPosition.UNKNOWN),
        new FieldReference(grouping.getGroupby(), ExpressionPosition.UNKNOWN));
      selection = new Selection(userTable, rowkeyRange, null, projections);
      mapList = new ArrayList<>(1);
      mapList.add(selection.toSelectionMap());
      userTableScan = new Scan(SE_HBASE, toJsonOptions(config, mapList), new FieldReference(userTable, ExpressionPosition.UNKNOWN));
      logicalOperators.add(userTableScan);
      if (hasSegment) {
        segmentLogicalOperator = buildSegment(projectId, userTable, dateString, logicalOperators, segmentMap);
        joinConditions = new JoinCondition[1];
        joinConditions[0] = new JoinCondition("==", buildColumn(eventTable, "uid"), buildColumn(userTable, "uid"));
        join = new Join(eventTableScan, segmentLogicalOperator, joinConditions, Join.JoinType.INNER);
        logicalOperators.add(join);

        JoinCondition[] joinConditions2 = new JoinCondition[1];
        joinConditions2[0] = new JoinCondition("==", buildColumn(userTable, "uid"), buildColumn(eventTable, "uid"));
        Join join2 = new Join(userTableScan, join, joinConditions2, Join.JoinType.RIGHT);
        logicalOperators.add(join2);
        scanRoot = join2;
      } else {
        joinConditions = new JoinCondition[1];
        joinConditions[0] = new JoinCondition("==", buildColumn(userTable, "uid"), buildColumn(eventTable, "uid"));
        join = new Join(userTableScan, eventTableScan, joinConditions, Join.JoinType.RIGHT);
        logicalOperators.add(join);
        scanRoot = join;
      }
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
        singleGroupByLE = functionRegistry.createExpression(func, null, new FieldReference(groupBy, null));
        groupByRef = eventTable + "." + grouping.getFunc() + "." + groupBy;
      } else if (GroupByType.EVENT.equals(groupByType)) {
        groupByRef = eventTable + "." + groupBy;
        singleGroupByLE = new FieldReference(groupByRef, null);
      } else {
        groupByRef = userTable + "." + groupBy;
        singleGroupByLE = new FieldReference(groupByRef, null);
      }
      fr = new FieldReference("segment_" + groupByRef, null);
      segment = new Segment(new NamedExpression[]{new NamedExpression(singleGroupByLE, fr)}, fr);
      segment.setInput(scanRoot);
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
        carryovers[0] = new FieldReference(eventTable + "." + grouping.getGroupby(), null);
      } else if (grouping.getGroupByType().equals(GroupByType.INTERNAL_FUNC)) {
        carryovers[0] = new FieldReference(grouping.getFunc() + "(" + eventTable + "." + grouping.getGroupby() + ")", null);
      } else {
        carryovers[0] = new FieldReference(userTable + "." + grouping.getGroupby(), null);
      }
    }

    String aggrColumn = eventTable + ".uid";
    FunctionCall fc;
    FieldReference aggrOn = new FieldReference(aggrColumn, null);
    fc = (FunctionCall) functionRegistry.createExpression(COUNT.getKeyWord(), null, aggrOn);
    namedExpressions[0] = new NamedExpression(fc, buildColumn("event_count"));

    fc = (FunctionCall) functionRegistry.createExpression(COUNT_DISTINCT.getKeyWord(), null, aggrOn);
    namedExpressions[1] = new NamedExpression(fc, buildColumn("user_number"));

    aggrColumn = eventTable + ".value";
    aggrOn = new FieldReference(aggrColumn, null);
    fc = (FunctionCall) functionRegistry.createExpression(SUM.getKeyWord(), null, aggrOn);
    namedExpressions[2] = new NamedExpression(fc, buildColumn("event_sum"));
    collapsingAggregate = new CollapsingAggregate(within, target, carryovers, namedExpressions);

    if (groupingQuery) {
      collapsingAggregate.setInput(segment);
    } else {
      collapsingAggregate.setInput(scanRoot);
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
    LogicalPlan logicalPlan = new LogicalPlan(buildPlanProperties(projectId), storageEngineMap, logicalOperators);
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

}
