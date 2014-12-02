package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.Constants;
import com.xingcloud.hbase.util.RowKeyUtils;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.util.EventTableUtil;
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
import org.apache.hadoop.hbase.regionserver.HBaseClientMultiScanner;
import org.apache.hadoop.hbase.regionserver.HBaseClientScanner;
import org.apache.hadoop.hbase.regionserver.XAScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

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
  private boolean useDFA = false; //是否需要DFA解析

  //记录row key中的投影
  private Map<String, HBaseFieldInfo> rowKeyProjs = new HashMap<>();
  //记录在family, qualifier, value, ts中所需要的投影
  private Map<String, HBaseFieldInfo> otherProjs = new HashMap<>();

  private Map<String, ValueVector> vvMap;

  private OutputMutator output;

  private XAScanner scanner;
  private List<KeyValue> curRes = new ArrayList<>();
  private int valIndex = 0;
  private int batchSize = 1024 * 64;
  private ValueVector[] valueVectors;
  private long scanCost = 0;
  private long parseCost = 0;
  private long setVectorCost = 0;
  boolean hasMore = true;
  int totalCount = 0;

  private Pair<byte[], byte[]> uidRange = new Pair<>();

  public HBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry config) {
    this.context = context;
    this.config = config;
  }

  private void initConfig() throws Exception {
    startRowKey = config.getStartRowKey();
    endRowKey = config.getEndRowKey();
    uidRange.setFirst(Arrays.copyOfRange(startRowKey, startRowKey.length - 5, startRowKey.length));
    uidRange.setSecond(Arrays.copyOfRange(endRowKey, endRowKey.length - 5, endRowKey.length));

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
          if (proInfo.serLength <= 0) {
            logger.debug("Need DFA parser to parse " + proInfo.fieldSchema.getName());
            useDFA = true;
          }
        } else {
          otherProjs.put(proInfo.fieldSchema.getName(), proInfo);
        }
      }
    }

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
    long start = System.nanoTime();
    FilterList filterList = new FilterList();
    List<KeyRange> slot = new ArrayList<>();

    List<HbaseScanPOP.RowkeyFilterEntry> filters = config.getFilters();
    if (filters != null) {
      Set<String> patterns = new HashSet<>();
      for (HbaseScanPOP.RowkeyFilterEntry entry : filters) {
        Constants.FilterType type = entry.getFilterType();
        switch (type) {
          case XaRowKeyPattern:
            for (String pattern : entry.getFilterExpressions()) {
                patterns.add(pattern);
            }
            break;
          case HbaseOrig:
            for (String filterExpr : entry.getFilterExpressions()) {
              LogicalExpression e =
                context.getDrillbitContext().getConfig().getMapper().readValue(filterExpr, LogicalExpression.class);
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
      filters = null;
      if (patterns.size() > 0) {       //todo should depend on hbase schema to generate row key
        List<String> sortedEvents = EventTableUtil.sortEventList(new ArrayList<>(patterns));
        patterns = null;

        for (String event : sortedEvents) {
          byte[] eventBytes = Bytes.toBytesBinary(event);
          byte[] lowerRange = Bytes.add(eventBytes, RowKeyUtils.produceTail(true));
          byte[] upperRange = Bytes.add(eventBytes, RowKeyUtils.produceTail(false));
          KeyRange keyRange = new KeyRange(lowerRange, true, upperRange, true);
          logger.debug("Add Key range: " + keyRange);
          slot.add(keyRange);
        }
        Filter skipScanFilter = new SkipScanFilter(slot, uidRange);
        filterList.addFilter(skipScanFilter);
        sortedEvents = null;
      }
    }
    config.setFilters(null);
    if (slot.size() == 0) {
      //如果没有key range，则需要加入start row和end row
      KeyRange keyRange = new KeyRange(startRowKey, true, endRowKey, false);
      slot.add(keyRange);
      logger.info("Slot size is 0 to skip uid range, add key range: " + keyRange);
      Filter skipScanFilter = new SkipScanFilter(slot, uidRange);
      filterList.addFilter(skipScanFilter);
    }

//    scanner = new DirectScanner(startRowKey, endRowKey, tableName, filterList, false, false);
      //test
    scanner= new HBaseClientScanner(startRowKey,endRowKey,tableName,filterList);
//      logger.info("Init HBaseClientMultiScanner begin");
//      scanner = new HBaseClientMultiScanner(startRowKey,endRowKey,tableName,filterList,slot);
      StringBuilder summary = new StringBuilder(tableName +"　StartKey: " + Bytes.toStringBinary(startRowKey) +
              "\tEndKey: " + Bytes.toStringBinary(endRowKey)  + "\tKey range size: " + slot.size());
      logger.info(summary.toString());
    logger.info("Init HBaseClientMultiScanner cost {} mills .", (System.nanoTime() - start) / 1000000);
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    try {
      initConfig();
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
    if (scanner == null) {
      try {
        initTableScanner();
      } catch (Exception e) {
        e.printStackTrace();
        throw new DrillRuntimeException("Init scanner failed .", e);
      }
    }
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
          scanCost += System.currentTimeMillis() - scannerStart;
        }
        if (curRes.size() == 0) {
          return endNext(recordSetIndex);
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new DrillRuntimeException("Scanner failed .", e);
      }
    }
  }

  private void setValues(List<KeyValue> keyValues, int offset, int length, int setIndex) {
    for (int i = offset; i < offset + length; i++) {
      try {
        setValues(keyValues.get(i), setIndex);
        setIndex++;
      } catch (Exception e) {
        logger.error("Ignore this keyvalue .");
        e.printStackTrace();
      }
    }
  }

  public int endNext(int valueCount) {
    totalCount += valueCount;
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
      dfaParser.parseAndSet(kv.getRow(), rowKeyProjs, vvMap, index, useDFA);
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
    logger.info("Record count for entry [tableName:{},keyRange:[{}:{}],count:{}]", config.getTableName(), Bytes.toStringBinary(config.getStartRowKey()), Bytes.toStringBinary(config.getEndRowKey()), totalCount);
    logger.info("HbaseRecordReader finished . ");
    for (int i = 0; i < valueVectors.length; i++) {
      try {
        output.removeField(valueVectors[i].getField());
      } catch (SchemaChangeException e) {
        logger.warn("Failure while trying to remove field.", e);
      }
      valueVectors[i].close();
    }
    try {
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
    } catch (Exception e) {
      logger.error("Scanners close failed : " + e.getMessage());
    }
    logger.debug("scan cost {} , parse cost {} ,setVectorCost {} ", scanCost, parseCost / 1000000, (setVectorCost - parseCost) / 1000000);
  }


}

