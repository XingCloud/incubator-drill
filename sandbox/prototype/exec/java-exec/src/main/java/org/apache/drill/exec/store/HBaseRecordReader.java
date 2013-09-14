package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.Constants;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import com.xingcloud.xa.hbase.filter.XARowKeyPatternFilter;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterCondition;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterPattern;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.parser.DFARowKeyParser;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.regionserver.HBaseClientScanner;
import org.apache.hadoop.hbase.regionserver.XAScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/23/13
 * Time: 12:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

  private HbaseScanPOP.HbaseScanEntry config;
  private FragmentContext context;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;

  private List<HBaseFieldInfo> projections;

  private Map<String, String> sourceRefMap;
  private List<KeyPart> primaryRowKeyParts;
  private Map<String, HBaseFieldInfo> fieldInfoMap;

  private DFARowKeyParser dfaParser;

  //记录row key中的投影
  private Map<String, HBaseFieldInfo> rowKeyProjs = new HashMap<>();
  //记录在family, qualifier, value, ts中所需要的投影
  private Map<String, HBaseFieldInfo> otherProjs = new HashMap<>();

  private Map<String, ValueVector> vvMap;


  private List<HbaseScanPOP.RowkeyFilterEntry> filters;
  private OutputMutator output;


  private XAScanner scanner;
  private List<KeyValue> curRes = new ArrayList<KeyValue>();
  private int valIndex = 0;
  private int batchSize = 1024 * 16;
  private ValueVector[] valueVectors;
  private long scanCost = 0;
  private long parseCost = 0;
  private long setVectorCost = 0;
  boolean hasMore = true;

  public HBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry config) {
    this.context = context;
    this.config = config;
  }

  private void initConfig() throws Exception {
    startRowKey = ByteUtils.toBytesBinary(config.getStartRowKey());
    endRowKey = ByteUtils.toBytesBinary(config.getEndRowKey());
    if (Arrays.equals(startRowKey, endRowKey))
      increaseBytesByOne(endRowKey);
    String tableFields[] = config.getTableName().split("\\.");
    tableName = tableFields[0];
    projections = new ArrayList<>();
    fieldInfoMap = new HashMap<>();
    sourceRefMap = new HashMap<>();
    List<NamedExpression> logProjection = config.getProjections();
    List<String> options = new ArrayList<>();
    for (int i = 0; i < logProjection.size(); i++) {
      options.add((String) ((SchemaPath) logProjection.get(i).getExpr()).getPath());
    }
    if (tableFields.length > 1) options.add(tableFields[1]);
    List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, options);
    for (HBaseFieldInfo col : cols) {
      fieldInfoMap.put(col.fieldSchema.getName(), col);
    }

    for (NamedExpression e : logProjection) {
      String ref = (String) e.getRef().getPath();
      String name = (String) ((SchemaPath) e.getExpr()).getPath();
      sourceRefMap.put(name, ref);

      if (!fieldInfoMap.containsKey(name)) {
        logger.debug("wrong field " + name + " hbase table has no this field");
      } else {
        HBaseFieldInfo proInfo = fieldInfoMap.get(name);
        projections.add(proInfo);
        if (proInfo.fieldType == HBaseFieldInfo.FieldType.rowkey) {
          rowKeyProjs.put(proInfo.fieldSchema.getName(), proInfo);
        } else {
          otherProjs.put(proInfo.fieldSchema.getName(), proInfo);
        }
      }
    }
    filters = config.getFilters();

    primaryRowKeyParts = TableInfo.getRowKey(tableName, options);
    dfaParser = new DFARowKeyParser(primaryRowKeyParts, fieldInfoMap);
  }

  public static void increaseBytesByOne(byte[] orig) {
    for (int i = orig.length - 1; i >= 0; i--) {
      orig[i]++;
      if (orig[i] != 0)
        break;
    }
  }

  private void initTableScanner() throws IOException {
    FilterList filterList = new FilterList();
    if (filters != null) {
      List<RowKeyFilterCondition> conditions = new ArrayList<>();
      List<String> patterns = new ArrayList<String>();
      for (HbaseScanPOP.RowkeyFilterEntry entry : filters) {
        Constants.FilterType type = entry.getFilterType();
        switch (type) {
          case XaRowKeyPattern:
            for (LogicalExpression e : entry.getFilterExpressions()) {
              String pattern = ((ValueExpressions.QuotedString) e).value;
              if (!patterns.contains(pattern)) {
                conditions.add(new RowKeyFilterPattern(((ValueExpressions.QuotedString) e).value));
                patterns.add(pattern);
              }
            }
            break;
          case HbaseOrig:
            for (LogicalExpression e : entry.getFilterExpressions()) {
              if (e instanceof FunctionCall) {
                FunctionCall c = (FunctionCall) e;
                Iterator iter = ((FunctionCall) e).iterator();
                SchemaPath leftField = (SchemaPath) iter.next();
                ValueExpressions.LongExpression rightField = (ValueExpressions.LongExpression) iter.next();
                HBaseFieldInfo info = fieldInfoMap.get(leftField.getPath());
                CompareFilter.CompareOp op = CompareFilter.CompareOp.GREATER;
                switch (c.getDefinition().getName()) {
                  case "greater than":
                    op = CompareFilter.CompareOp.GREATER;
                    break;
                  case "less than":
                    op = CompareFilter.CompareOp.LESS;
                    break;
                  case "equal":
                    op = CompareFilter.CompareOp.EQUAL;
                    break;
                  case "greater than or equal to":
                    op = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                    break;
                  case "less than or equal to":
                    op = CompareFilter.CompareOp.LESS_OR_EQUAL;
                    break;
                }
                switch (info.fieldType) {
                  case cellvalue:
                    String cfName = info.cfName;
                    String cqName = info.cqName;
                    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
                      Bytes.toBytes(cfName),
                      Bytes.toBytes(cqName),
                      op,
                      new BinaryComparator(Bytes.toBytes(rightField.getLong()))
                    );
                    filterList.addFilter(valueFilter);
                    break;
                  case cqname:
                    Filter qualifierFilter =
                      new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(rightField.getLong())));
                    filterList.addFilter(qualifierFilter);
                  default:
                    break;
                }

              }
            }
            break;
          default:
            throw new IllegalArgumentException("unsupported filter type:" + type);
        }
      }
      if (conditions.size() >= 1) {
        XARowKeyPatternFilter xaFilter = new XARowKeyPatternFilter(conditions);
        filterList.addFilter(xaFilter);
      }
    }

    scanner = new DirectScanner(startRowKey, endRowKey, tableName, filterList, false, false);
    //test
    //scanner=new HBaseClientScanner(startRowKey,endRowKey,tableName,filterList);
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    try {
      initConfig();
      initTableScanner();
      valueVectors = new ValueVector[projections.size()];
      vvMap = new HashMap<>(valueVectors.length);
      for (int i = 0; i < projections.size(); i++) {
        MajorType type = getMajorType(projections.get(i));
        valueVectors[i] =
          getVector(sourceRefMap.get(projections.get(i).fieldSchema.getName()), type);
        output.addField(valueVectors[i]);
        output.setNewSchema();
        vvMap.put(projections.get(i).fieldSchema.getName(), valueVectors[i]);
      }

    } catch (Exception e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
  }

  public static MajorType getMajorType(HBaseFieldInfo info) {
    String type = info.fieldSchema.getType();
    switch (type) {
      case "int":
        return Types.required(MinorType.INT);
      case "tinyint":
        return Types.required(MinorType.UINT1);
      case "string":
        return Types.required(MinorType.VARCHAR);
      case "bigint":
        return Types.required(MinorType.BIGINT);
      case "smallint":
        return Types.required(MinorType.SMALLINT);
    }
    return null;
  }

  private ValueVector getVector(String name, MajorType type) {
    if (type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();
    MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
    if (context == null) return TypeHelper.getNewVector(f, new DirectBufferAllocator());
    return TypeHelper.getNewVector(f, context.getAllocator());
  }


  public int next() {
    for (ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, batchSize, 4);
    }
    int recordSetIndex = 0;
    while (true) {
      if (valIndex < curRes.size()) {
        int length = Math.min(batchSize - recordSetIndex, curRes.size() - valIndex);
        setValues(curRes, valIndex, length, recordSetIndex);
        recordSetIndex += length;
        if (valIndex + length != curRes.size()) {
          valIndex += length;
          return endNext(recordSetIndex);
        } else {
          valIndex = 0;
          curRes.clear();
        }
      }
      try {
        if (hasMore) {
          long scannerStart = System.currentTimeMillis();
          hasMore = scanner.next(curRes);
          scanCost += System.currentTimeMillis() - scannerStart ;
        }
        if (curRes.size() == 0) {
          return endNext(recordSetIndex);
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new DrillRuntimeException("Scanner failed");
      }
    }
  }

  private void setValues(List<KeyValue> keyValues, int offset, int length, int setIndex) {
    for (int i = offset; i < offset + length; i++) {
      setValues(keyValues.get(i), setIndex);
      setIndex++;
    }
  }

  public int endNext(int valueCount) {
    if (valueCount == 0)
      return 0;
    setValueCount(valueCount);
    return valueCount;
  }

  public void setValues(KeyValue kv, int index) {
    long setVecotorStart = System.nanoTime();
    long parseStart = System.nanoTime();
    //填充row key中的投影
    if (rowKeyProjs.size() != 0) {
      dfaParser.parseAndSet(kv.getRow(), rowKeyProjs, vvMap, index);
    }
    parseCost += System.nanoTime() - parseStart;

    //更新family，qualifier，ts和value的投影值
    for (Map.Entry<String, HBaseFieldInfo> entry : otherProjs.entrySet()) {
      String colName = entry.getKey();
      ValueVector vv = vvMap.get(colName);
      HBaseFieldInfo info = entry.getValue();
      Object value = null;
      if (info.fieldType == HBaseFieldInfo.FieldType.cellvalue) {
        value = DFARowKeyParser.parseBytes(kv.getValue(), info.getDataType());
      } else if (info.fieldType == HBaseFieldInfo.FieldType.cqname) {
        value = DFARowKeyParser.parseBytes(kv.getQualifier(), info.getDataType());
      } else if (info.fieldType == HBaseFieldInfo.FieldType.cversion) {
        value = kv.getTimestamp();
      }
      vv.getMutator().setObject(index, value);
    }
    setVectorCost += System.nanoTime() - setVecotorStart;
  }

  private void setValueCount(int valueCount) {
    for (int i = 0; i < valueVectors.length; i++) {
      valueVectors[i].getMutator().setValueCount(valueCount);
    }
  }


  @Override
  public void cleanup() {
    for (int i = 0; i < valueVectors.length; i++) {
      try {
        output.removeField(valueVectors[i].getField());
      } catch (SchemaChangeException e) {
        logger.warn("Failure while trying to remove field.", e);
      }
      valueVectors[i].close();
    }
    try {
      scanner.close();
    } catch (Exception e) {
      logger.error("Scanners close failed : " + e.getMessage());
    }
    logger.debug("scan cost {} , parse cost {} ,setVectorCost {} ", scanCost, parseCost / 1000000, (setVectorCost - parseCost) / 1000000);
  }


}

