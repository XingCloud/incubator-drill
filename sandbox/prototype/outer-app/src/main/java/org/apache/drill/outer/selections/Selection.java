package org.apache.drill.outer.selections;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.xingcloud.meta.KeyPart.Type;
import static org.apache.drill.common.expression.ValueExpressions.LongExpression;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTERS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROJECTIONS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_END;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_START;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;
import static org.apache.drill.outer.selections.GenericUtils.GENERIC_OBJECT_MAPPER;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.meta.DefaultDrillHiveMetaClient;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-7-24 Time: 上午10:49 Package: org.apache.drill.common.util
 */

@JsonInclude(NON_NULL)
public class Selection {

  private static DefaultDrillHiveMetaClient CLIENT;

  static {
    try {
      CLIENT = DefaultDrillHiveMetaClient.createClient();
    } catch (MetaException e) {
      e.printStackTrace();
    }
  }

  public static enum KeyPartParameterType {
    SINGLE, RANGE
  }

  public static class KeyPartParameter {
    private String parameterValue1;
    private String parameterValue2;
    private KeyPartParameterType parameterValueType;

    public KeyPartParameter(String parameterValue1, KeyPartParameterType parameterValueType) {
      this.parameterValue1 = parameterValue1;
      this.parameterValueType = parameterValueType;
    }

    public KeyPartParameter(String parameterValue1, String parameterValue2, KeyPartParameterType parameterValueType) {
      this.parameterValue1 = parameterValue1;
      this.parameterValue2 = parameterValue2;
      this.parameterValueType = parameterValueType;
    }

    public boolean isSingle() {
      return getParameterValueType().equals(KeyPartParameterType.SINGLE);
    }

    public String getParameterValue1() {
      return parameterValue1;
    }

    public String getParameterValue2() {
      return parameterValue2;
    }

    public KeyPartParameterType getParameterValueType() {
      return parameterValueType;
    }

    public static KeyPartParameter buildSingleKey(String parameterValue) {
      return new KeyPartParameter(parameterValue, KeyPartParameterType.SINGLE);
    }

    public static KeyPartParameter buildRangeKey(String parameterValue1, String parameterValue2) {
      return new KeyPartParameter(parameterValue1, parameterValue2, KeyPartParameterType.RANGE);
    }
  }

  public static class RowkeyRange {

    protected String startKey;
    protected String endKey;

    public RowkeyRange(String startKey, String endKey) {
      this.startKey = startKey;
      this.endKey = endKey;
    }

    public String getStartKey() {
      return startKey;
    }

    public String getEndKey() {
      return endKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowkeyRange)) {
        return false;
      }

      RowkeyRange that = (RowkeyRange) o;

      if (!endKey.equals(that.endKey)) {
        return false;
      }
      if (!startKey.equals(that.startKey)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = startKey.hashCode();
      result = 31 * result + endKey.hashCode();
      return result;
    }

    private static boolean validate(String startKey, String endKey) {
      if (StringUtils.isBlank(startKey) || StringUtils.isBlank(endKey)) {
        return false;
      }
      return true;
    }

    public static RowkeyRange create(String startKey, String endKey) {
      if (validate(startKey, endKey)) {
        return new RowkeyRange(startKey, endKey);
      }
      throw new IllegalArgumentException("Start key or end key can not be empty - " + startKey + " - " + endKey);
    }

    @Override
    public String toString() {
      return "PK(" + startKey + ',' + endKey + ")";
    }
  }

  private String dbName;
  private String tableName;

  private RowkeyRange rowkey;

  private LogicalExpression[] fieldFilters;

  private NamedExpression[] projections;

  public Selection(String dbName, String tableName, RowkeyRange rowkey, NamedExpression[] projections) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.projections = projections;
  }

  public Selection(String dbName, String tableName, RowkeyRange rowkey, LogicalExpression[] fieldFilters,
                   NamedExpression[] projections) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.fieldFilters = fieldFilters;
    this.projections = projections;
  }

  public Selection(String tableName, RowkeyRange rowkey, NamedExpression[] projections) {
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.projections = projections;
  }

  public Selection(String tableName, RowkeyRange rowkey, LogicalExpression[] fieldFilters,
                   NamedExpression[] projections) {
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.fieldFilters = fieldFilters;
    this.projections = projections;
  }

  private static void extractColumns(List<KeyPart> pkKeyParts, List<KeyPart> allKeyParts) {
    Type type;
    for (KeyPart kp : pkKeyParts) {
      type = kp.getType();
      if (Type.constant.equals(type) || Type.field.equals(type)) {
        allKeyParts.add(kp);
      }
      // Optional Group
      else {
        List<KeyPart> optional = kp.getOptionalGroup();
        extractColumns(optional, allKeyParts);
      }
    }
  }

  public static RowkeyRange resolveRowkeyRange(String tableName, Map<String, KeyPartParameter> parameterMap,
                                               int[] parameterLength) throws NoSuchObjectException, TException {
    Table table = CLIENT.getTable("test_xa", tableName);

    String pattern = TableInfo.getPrimaryKeyPattern(table);
    List<KeyPart> pkKeyParts = TableInfo.getPrimaryKey(table), allKeyParts = new ArrayList<>();

    extractColumns(pkKeyParts, allKeyParts);

    StringBuilder startKeySB = new StringBuilder(), endKeySB = new StringBuilder();

    Type keyPartType;
    // There has no optional keys.
    String fieldName, keyPartValue1, keyPartValue2;
    KeyPartParameter keyPartParameter;

    int resolvedStartCount = 0, resolvedEndCount = 0;
    int startParamSize = parameterLength[0], endParamSize = parameterLength[1];

    for (KeyPart kp : allKeyParts) {
      keyPartType = kp.getType();
      if (Type.field.equals(keyPartType)) {
        fieldName = kp.getField().getName();
        keyPartParameter = parameterMap.get(fieldName);
        if (keyPartParameter == null) {
          continue;
        }
        if (keyPartParameter.isSingle()) {
          keyPartValue1 = keyPartParameter.getParameterValue1();
          if (keyPartValue1 == null) {
            break;
          } else {
            if (resolvedStartCount < startParamSize) {
              startKeySB.append(keyPartValue1);
              ++resolvedStartCount;
            }
            if (resolvedEndCount < endParamSize) {
              endKeySB.append(keyPartValue1);
              ++resolvedEndCount;
            }
          }
        } else {
          keyPartValue1 = keyPartParameter.getParameterValue1();
          keyPartValue2 = keyPartParameter.getParameterValue2();
          if (resolvedStartCount < startParamSize) {
            startKeySB.append(keyPartValue1);
            ++resolvedStartCount;
          }
          if (resolvedEndCount < endParamSize) {
            endKeySB.append(keyPartValue2);
            ++resolvedEndCount;
          }
        }
      } else {
        if (resolvedStartCount < startParamSize) {
          startKeySB.append(kp.getConstant());
        }
        if (resolvedEndCount < endParamSize) {
          endKeySB.append(kp.getConstant());
        }
      }
    }
    return new RowkeyRange(startKeySB.toString(), endKeySB.toString());
  }

  // int[0] = start keypart parameter length
  // int[1] = end keypart parameter length
  private static int[] keyPartParameterLength(Map<String, KeyPartParameter> parameterMap) {
    int startParameterCount = 0;
    int endParameterCount = 0;
    KeyPartParameter kpp;
    String v1, v2;
    for (Map.Entry<String, KeyPartParameter> entry : parameterMap.entrySet()) {
      kpp = entry.getValue();
      if (kpp.isSingle()) {
        ++startParameterCount;
        ++endParameterCount;
      } else {
        v1 = kpp.getParameterValue1();
        v2 = kpp.getParameterValue2();
        if (v1 != null) {
          ++startParameterCount;
        }
        if (v2 != null) {
          ++endParameterCount;
        }
      }
    }
    return new int[]{startParameterCount, endParameterCount};
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public RowkeyRange getRowkey() {
    return rowkey;
  }

  public void setRowkey(RowkeyRange rowkey) {
    this.rowkey = rowkey;
  }

  public LogicalExpression[] getFieldFilters() {
    return fieldFilters;
  }

  public void setFieldFilters(LogicalExpression[] fieldFilters) {
    this.fieldFilters = fieldFilters;
  }

  public NamedExpression[] getProjections() {
    return projections;
  }

  public void setProjection(NamedExpression[] projections) {
    this.projections = projections;
  }

  public Map<String, Object> toSelectionMap() throws IOException {
    Map<String, Object> map = new HashMap<>(4);
    map.put(SELECTION_KEY_WORD_TABLE, this.getTableName());

    Map<String, Object> rowkeyRangeMap = new HashMap<>(2);
    rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_START, this.getRowkey().getStartKey());
    rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_END, this.getRowkey().getEndKey());
    map.put(SELECTION_KEY_WORD_ROWKEY, rowkeyRangeMap);
    map.put(SELECTION_KEY_WORD_PROJECTIONS, this.getProjections());
    map.put(SELECTION_KEY_WORD_FILTERS, this.getFieldFilters());
    return map;
  }



  public static void main(String[] args) throws IOException, TException, RecognitionException {
    LogicalExpression le=new ValueExpressions.LongExpression(1l,ExpressionPosition.UNKNOWN);
    System.out.println(le);
//    Map<String, KeyPartParameter> parameterMap = new HashMap<>(5);
//    parameterMap.put("date", KeyPartParameter.buildSingleKey("20130726"));
//    // Real event is from a.001 to a.999.x.z, transform to these entries.
//    parameterMap.put("event0", KeyPartParameter.buildSingleKey("visit"));
//    parameterMap.put("event1", KeyPartParameter.buildRangeKey("001", "999"));
//    parameterMap.put("event2", KeyPartParameter.buildRangeKey("v", null));
//    parameterMap.put("event3", KeyPartParameter.buildRangeKey("b", null));
//
//    int[] parameterLength = keyPartParameterLength(parameterMap);
//    RowkeyRange rowkeyRange = Selection.resolveRowkeyRange("deu_age", parameterMap, parameterLength);
//    ObjectMapper mapper = new ObjectMapper();
//
//    String dbname = "text_xa";
//    String tableName = "deu_age";
//
//    LogicalExpression[] filters = new LogicalExpression[2];
//    LogicalExpression value = new LongExpression(123l, null);
//    filters[0] = new FunctionRegistry(DrillConfig.create()).createExpression(">", null, new FieldReference("deu_age.value", null), value);
//
//    value = new LongExpression(new Date().getTime(), null);
//    filters[1] = new FunctionRegistry(DrillConfig.create()).createExpression("=", null, new FieldReference("deu_age.version", null), value);
//
//    NamedExpression[] projections = new NamedExpression[1];
//    projections[0] = new NamedExpression(new FieldReference("deu_age.uid", null), new FieldReference("deu_age.uid", null));
//    Selection selection = new Selection(dbname, tableName, rowkeyRange, filters,
//      projections);
//
//    Map<String, Object> map = selection.toSelectionMap();
//    List<Map<String, Object>> mapList = new ArrayList<>(1);
//    mapList.add(map);
//
//    String s = GENERIC_OBJECT_MAPPER.writeValueAsString(mapList);
//    System.out.println(s);
//    JSONOptions options = GENERIC_OBJECT_MAPPER.readValue(GENERIC_OBJECT_MAPPER.writeValueAsBytes(mapList), JSONOptions.class);
//
//    JsonNode rootNode = options.getRoot();
//    System.out.println(rootNode.size());
//
//    JsonNode filterss, projectionss, rowkey;
//    String table, rowkeyStart, rowkeyEnd, projectionRef, projectionExpr;
//    for (JsonNode child : rootNode) {
//      table = child.get(SELECTION_KEY_WORD_TABLE).textValue();
//      System.out.println(table);
//
//      rowkey = child.get(SELECTION_KEY_WORD_ROWKEY);
//
//      rowkeyStart = rowkey.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
//      rowkeyEnd = rowkey.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();
//      System.out.println(rowkeyStart + " to " + rowkeyEnd);
//
//      filterss = child.get(SELECTION_KEY_WORD_FILTERS);
//      projectionss = child.get(SELECTION_KEY_WORD_PROJECTIONS);
//    }
//    ExprLexer lexer = new ExprLexer(new ANTLRStringStream("a>1"));
//
//    CommonTokenStream tokens = new CommonTokenStream(lexer);
//    ExprParser parser = new ExprParser(tokens);
//    ExprParser.parse_return ret = parser.parse();
//    LogicalExpression e = ret.e;
//    System.out.println(e);
  }

}
