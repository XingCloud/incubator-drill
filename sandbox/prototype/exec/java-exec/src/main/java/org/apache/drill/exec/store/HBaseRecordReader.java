package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.Constants;
import com.xingcloud.hbase.util.DFARowKeyParser;
import com.xingcloud.hbase.util.RowKeyUtils;
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
import org.apache.drill.exec.physical.impl.unionedscan.MultiEntryHBaseRecordReader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.regionserver.TableScanner;
import org.apache.hadoop.hbase.regionserver.XAScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
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
  private boolean parseRk = false;

  private DFARowKeyParser dfaParser;

  private List<HbaseScanPOP.RowkeyFilterEntry> filters;
  private OutputMutator output;


  private List<XAScanner> scanners = new ArrayList<>();
  private int currentScannerIndex = 0;
  private List<KeyValue> curRes = new ArrayList<KeyValue>();
  private int valIndex = -1;
  private boolean hasMore;
  private int batchSize = 1024 * 16;
  private ValueVector[] valueVectors;
  private boolean init = false;
  private long timeCost = 0;
  private long scanCost = 0 ;
  private long parseCost = 0 ;
  private long setVectorCost = 0 ;
  private long timeStart;

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
        if (false == parseRk && proInfo.fieldType == HBaseFieldInfo.FieldType.rowkey)
          parseRk = true;
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
    scanners = new ArrayList<>();
    FilterList filterList = new FilterList();
    if (filters != null) {
      List<RowKeyFilterCondition> conditions=new ArrayList<>();
      List<String>  patterns=new ArrayList<String>();
      for (HbaseScanPOP.RowkeyFilterEntry entry : filters) {
       Constants.FilterType type = entry.getFilterType();
        switch (type) {
          case XaRowKeyPattern:
            for (LogicalExpression e : entry.getFilterExpressions()) {
              String pattern=((ValueExpressions.QuotedString)e).value;
              if(!patterns.contains(pattern)){
                conditions.add(new RowKeyFilterPattern(((ValueExpressions.QuotedString)e).value));
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
      if(conditions.size()>=1){
        XARowKeyPatternFilter xaFilter = new XARowKeyPatternFilter(conditions);
        filterList.addFilter(xaFilter);
      }
    }

    XAScanner scanner=new DirectScanner(startRowKey, endRowKey, tableName, filterList, false, false);
    //test
    //XAScanner scanner=new TableScanner(startRowKey,endRowKey,tableName,filterList,false,false);
    scanners.add(scanner);
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    try {
      initConfig();
      initTableScanner();
      valueVectors = new ValueVector[projections.size()];
      for (int i = 0; i < projections.size(); i++) {
        MajorType type = getMajorType(projections.get(i));
        valueVectors[i] =
          getVector(sourceRefMap.get(projections.get(i).fieldSchema.getName()), type);
        output.addField(valueVectors[i]);
        output.setNewSchema();
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


  @Override
  public int next() {
    timeStart = System.currentTimeMillis();
    for (ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, batchSize, 8);
    }

    int recordSetIndex = 0;
    while (true) {
      if (currentScannerIndex > scanners.size() - 1) {
        setValueCount(recordSetIndex);
        timeCost += System.currentTimeMillis() - timeStart;
        return recordSetIndex;
      }
      XAScanner scanner = scanners.get(currentScannerIndex);
      if (valIndex == -1) {
        if (scanner == null) {
          timeCost += System.currentTimeMillis() - timeStart;
          return 0;
        }
        try {
          long scanStart = System.currentTimeMillis();
          hasMore = scanner.next(curRes);
          scanCost += System.currentTimeMillis() - scanStart ;

        } catch (IOException e) {
          throw new DrillRuntimeException("Scan hbase failed : " + e.getMessage());
        }
        valIndex = 0;
      }
      if (valIndex > curRes.size() - 1) {
        if (!hasMore) {
          currentScannerIndex++;
          valIndex = -1;
          continue;
        }
        while (hasMore) {
                        /* Get result list from the same scanner and skip curRes with no element */
          curRes.clear();
          try {
            long scanStart = System.currentTimeMillis();
            hasMore = scanner.next(curRes);
            scanCost += System.currentTimeMillis() - scanStart;
          } catch (IOException e) {
            e.printStackTrace();
          }
          valIndex = 0;
          if (!hasMore) currentScannerIndex++;
          if (curRes.size() != 0) {
            KeyValue kv = curRes.get(valIndex++);
            boolean next = setValues(kv, valueVectors, recordSetIndex);
            recordSetIndex++;
            if (!next) {
              setValueCount(recordSetIndex);
              timeCost += System.currentTimeMillis() - timeStart;
              return recordSetIndex;

            }
            break;
          }
        }
        if (valIndex > curRes.size() - 1) {
          if (!hasMore) valIndex = -1;
          continue;
        }

      }
      KeyValue kv = curRes.get(valIndex++);
      boolean next = setValues(kv, valueVectors, recordSetIndex);
      recordSetIndex++;
      if (!next) {
        setValueCount(recordSetIndex);
        timeCost += System.currentTimeMillis() - timeStart ;
        return recordSetIndex;
      }

    }
  }

  public boolean setValues(KeyValue kv, ValueVector[] valueVectors, int index) {
    long setVecotorStart = System.nanoTime();
    boolean next = true;
    Map<String, Object> rkObjectMap = new HashMap<>();
    long parseStart = System.nanoTime() ;
    if (parseRk) rkObjectMap = dfaParser.parse(kv.getRow());
    parseCost +=  System.nanoTime() - parseStart ;
    for (int i = 0; i < projections.size(); i++) {
      HBaseFieldInfo info = projections.get(i);
      ValueVector valueVector = valueVectors[i];
      parseStart = System.nanoTime() ;
      Object result = getValFromKeyValue(kv, info, rkObjectMap);
      parseCost += System.nanoTime() - parseStart ;
      String type = info.fieldSchema.getType();
      if (type.equals("string"))
        result = Bytes.toBytes((String) result);
      valueVector.getMutator().setObject(index, result);
      if (valueVector.getValueCapacity() - index == 1) {
        next = false;
      }
    }
    setVectorCost += System.nanoTime() - setVecotorStart ;
    return next;
  }

  private void setValueCount(int valueCount) {
    for (int i = 0; i < valueVectors.length; i++) {
      valueVectors[i].getMutator().setValueCount(valueCount);
    }
  }

  public static Object getValFromKeyValue(KeyValue keyvalue, HBaseFieldInfo option, Map<String, Object> rkObjectMap) {
    String fieldName = option.fieldSchema.getName();
    if (option.fieldType == HBaseFieldInfo.FieldType.rowkey) {
      if (!rkObjectMap.containsKey(fieldName))
        logger.info("error! " + fieldName + " does not exists in this keyvalue");
      else
        return rkObjectMap.get(fieldName);
    } else if (option.fieldType == HBaseFieldInfo.FieldType.cellvalue) {
      String cfName = Bytes.toString(keyvalue.getFamily());
      String cqName = Bytes.toString(keyvalue.getQualifier());
      if (!option.cfName.equals(cfName) || !option.cqName.equals(cqName))
        logger.info("error! this field's column info---" + option.cqName + ":" + option.cqName +
          " does not match the keyvalue's column info---" + cfName + ":" + cqName);
      else {
        return DFARowKeyParser.parseBytes(keyvalue.getValue(), option.fieldSchema.getType());
      }
    } else if (option.fieldType == HBaseFieldInfo.FieldType.cversion) {
      return keyvalue.getTimestamp();
    } else if (option.fieldType == HBaseFieldInfo.FieldType.cqname) {
      byte[] qualif = keyvalue.getQualifier();
      byte[] orig = new byte[4];
      for (int i = 0; i < 4; i++)
        orig[i] = keyvalue.getQualifier()[i + 1];
      return DFARowKeyParser.parseBytes(orig, option.fieldSchema.getType());
    }
    return null;
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
    for (XAScanner scanner : scanners) {
      try {
        scanner.close();
      } catch (Exception e) {
        logger.error("Scanners close failed : " + e.getMessage());
      }
    }
    logger.debug("Scan and parse cost {} ,scan cost {} , parse cost {} ,setVectorCost {} ",timeCost,scanCost,parseCost/1000000,(setVectorCost - parseCost)/1000000);
  }


}

