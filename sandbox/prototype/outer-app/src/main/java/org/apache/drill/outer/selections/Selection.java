package org.apache.drill.outer.selections;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.xingcloud.meta.KeyPart.Type;
import static org.apache.drill.common.expression.ValueExpressions.LongExpression;
import static org.apache.drill.common.util.Selections.*;
import static org.apache.drill.outer.utils.GenericUtils.addAll;
import static org.apache.drill.outer.utils.GenericUtils.toBytes;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.meta.DefaultDrillHiveMetaClient;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.antlr.runtime.RecognitionException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.util.*;

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
    private byte[] parameterValue1;
    private byte[] parameterValue2;
    private KeyPartParameterType parameterValueType;

    public KeyPartParameter(byte[] parameterValue1, KeyPartParameterType parameterValueType) {
      this.parameterValue1 = parameterValue1;
      this.parameterValueType = parameterValueType;
    }

    public KeyPartParameter(byte[] parameterValue1, byte[] parameterValue2, KeyPartParameterType parameterValueType) {
      this.parameterValue1 = parameterValue1;
      this.parameterValue2 = parameterValue2;
      this.parameterValueType = parameterValueType;
    }

    public boolean isSingle() {
      return getParameterValueType().equals(KeyPartParameterType.SINGLE);
    }

    public byte[] getParameterValue1() {
      return parameterValue1;
    }

    public byte[] getParameterValue2() {
      return parameterValue2;
    }

    public KeyPartParameterType getParameterValueType() {
      return parameterValueType;
    }

    public static KeyPartParameter buildSingleKey(byte[] parameterValue) {
      return new KeyPartParameter(parameterValue, KeyPartParameterType.SINGLE);
    }

    public static KeyPartParameter buildRangeKey(byte[] parameterValue1, byte[] parameterValue2) {
      return new KeyPartParameter(parameterValue1, parameterValue2, KeyPartParameterType.RANGE);
    }
  }

  public static class RowkeyRange {

    protected byte[] startKey;
    protected byte[] endKey;

    public RowkeyRange(byte[] startKey, byte[] endKey) {
      this.startKey = startKey;
      this.endKey = endKey;
    }

    public byte[] getStartKey() {
      return startKey;
    }

    public byte[] getEndKey() {
      return endKey;
    }


    private static boolean validate(byte[] startKey, byte[] endKey) {
      if (ArrayUtils.isEmpty(startKey) || ArrayUtils.isEmpty(endKey)) {
        return false;
      }
      return true;
    }

    public static RowkeyRange create(byte[] startKey, byte[] endKey) {
      if (validate(startKey, endKey)) {
        return new RowkeyRange(startKey, endKey);
      }
      throw new IllegalArgumentException("Start key or end key can not be empty - " + startKey + " - " + endKey);
    }

    @Override
    public String toString() {
      BASE64Encoder encoder = new BASE64Encoder();
      return "PK(" + encoder.encode(startKey) + ',' + encoder.encode(endKey) + ")";
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

  public static RowkeyRange resolveRowkeyRange(String tableName, Map<String, KeyPartParameter> parameterMap
  ) throws NoSuchObjectException, TException {
    Table table = CLIENT.getTable("test_xa", tableName);
    int[] parameterLength = keyPartParameterLength(parameterMap);
    List<KeyPart> pkKeyParts = TableInfo.getPrimaryKey(table), allKeyParts = new ArrayList<>();

    extractColumns(pkKeyParts, allKeyParts);

    List<Byte> startKeyList = new ArrayList<>(20), endKeyList = new ArrayList<>(20);
    Type keyPartType;
    // There has no optional keys.
    String fieldName;
    byte[] keyPartValue1, keyPartValue2;
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
              addAll(startKeyList, keyPartValue1);
              ++resolvedStartCount;
            }
            if (resolvedEndCount < endParamSize) {
              addAll(endKeyList, keyPartValue1);
              ++resolvedEndCount;
            }
          }
        } else {
          keyPartValue1 = keyPartParameter.getParameterValue1();
          keyPartValue2 = keyPartParameter.getParameterValue2();
          if (resolvedStartCount < startParamSize) {
            addAll(startKeyList, keyPartValue1);
            ++resolvedStartCount;
          }
          if (resolvedEndCount < endParamSize) {
            addAll(endKeyList, keyPartValue2);
            ++resolvedEndCount;
          }
        }
      } else {
        if (resolvedStartCount < startParamSize) {
          addAll(startKeyList, kp.getConstant().getBytes());
        }
        if (resolvedEndCount < endParamSize) {
          addAll(endKeyList, kp.getConstant().getBytes());
        }
      }
    }
    int size = startKeyList.size();
    byte[] startKey = new byte[size];
    for (int i = 0; i < size; i++) {
      startKey[i] = startKeyList.get(i);
    }
    size = endKeyList.size();
    byte[] endKey = new byte[size];
    for (int i = 0; i < size; i++) {
      endKey[i] = endKeyList.get(i);
    }
    return new RowkeyRange(startKey, endKey);
  }

  // int[0] = start keypart parameter length
  // int[1] = end keypart parameter length
  private static int[] keyPartParameterLength(Map<String, KeyPartParameter> parameterMap) {
    int startParameterCount = 0;
    int endParameterCount = 0;
    KeyPartParameter kpp;
    byte[] v1, v2;
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

  public JSONOptions toSingleJsonOptions() throws IOException {
    List<Map<String, Object>> mapList = new ArrayList<>(1);
    mapList.add(this.toSelectionMap());
    ObjectMapper mapper = DrillConfig.create().getMapper();
    String s = mapper.writeValueAsString(mapList);
    return mapper.readValue(s, JSONOptions.class);
  }

  public static void testEvent() throws IOException, TException {
    Map<String, KeyPartParameter> parameterMap = new HashMap<>(5);
    parameterMap.put("date", KeyPartParameter.buildSingleKey(toBytes("20130801")));
    // Real event is from a.001 to a.999.x.z, transform to these entries.
    parameterMap.put("event0", KeyPartParameter.buildSingleKey(toBytes("visit")));
    parameterMap.put("event1", KeyPartParameter.buildRangeKey(toBytes("001"), toBytes("999")));
    parameterMap.put("event2", KeyPartParameter.buildRangeKey(toBytes("v"), null));
    parameterMap.put("event3", KeyPartParameter.buildRangeKey(toBytes("b"), null));

    RowkeyRange rowkeyRange = Selection.resolveRowkeyRange("deu_age", parameterMap);

    String dbname = "text_xa";
    String tableName = "deu_age";

    LogicalExpression[] filters = new LogicalExpression[2];
    LogicalExpression value = new LongExpression(123l, null);
    filters[0] = new FunctionRegistry(DrillConfig.create()).createExpression(">", null, new FieldReference("deu_age.value", null), value);

    value = new LongExpression(new Date().getTime(), null);
    filters[1] = new FunctionRegistry(DrillConfig.create()).createExpression("=", null, new FieldReference("deu_age.version", null), value);

    NamedExpression[] projections = new NamedExpression[1];
    projections[0] = new NamedExpression(new FieldReference("deu_age.uid", null), new FieldReference("deu_age.uid", null));
    Selection selection = new Selection(dbname, tableName, rowkeyRange, filters,
      projections);

    Map<String, Object> map = selection.toSelectionMap();
    List<Map<String, Object>> mapList = new ArrayList<>(1);
    mapList.add(map);
    ObjectMapper mapper = DrillConfig.create().getMapper();
    String s = mapper.writeValueAsString(mapList);
    System.out.println(s);
  }

  public static void testUser() throws NoSuchObjectException, TException, JsonProcessingException {
    DrillConfig config = DrillConfig.create();
    String dbname = "text_xa";
    String tableName = "user_index_age";

    Map<String, KeyPartParameter> parameterMap = new HashMap<>(5);
    parameterMap.put("propnumber", KeyPartParameter.buildSingleKey(toBytes(65)));
    parameterMap.put("propnumber", KeyPartParameter.buildSingleKey(toBytes("20130801")));
    RowkeyRange rowkeyRange = Selection.resolveRowkeyRange("user_index_age", parameterMap);

    System.out.println(config.getMapper().writeValueAsString(rowkeyRange));
    System.out.println(rowkeyRange);

  }


  public static void main(String[] args) throws IOException, TException, RecognitionException {
    testUser();
  }

}
