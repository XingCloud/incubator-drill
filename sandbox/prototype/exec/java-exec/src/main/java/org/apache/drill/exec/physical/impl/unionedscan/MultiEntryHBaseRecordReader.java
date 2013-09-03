package org.apache.drill.exec.physical.impl.unionedscan;

import com.xingcloud.hbase.util.DFARowKeyParser;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.HBaseRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultiEntryHBaseRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MultiEntryHBaseRecordReader.class);
  private HbaseScanPOP.HbaseScanEntry[] entries;
  private Pair<byte[],byte[]>[] entryKeys ;

  private FragmentContext context;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  private List<List<HbaseScanPOP.RowkeyFilterEntry>> entryFilters;

  private ValueVector entryIndexVector;
  private List<NamedExpression> projections;
  private List<NamedExpression[]> entryProjections;
  private List<HBaseFieldInfo[]> entryProjFieldInfos = new ArrayList<>();
  private Map<String, HBaseFieldInfo> fieldInfoMap;
  private List<ValueVector> valueVectors;
  private OutputMutator outputMutator;
  private boolean parseRk = false;
  private int batchSize = 1024 * 16;

  private boolean newEntry = false;

  private DirectScanner scanner;
  private int valIndex = -1;
  private List<KeyValue> curRes = new ArrayList<>();
  private List<KeyPart> primaryRowKeyParts;
  private DFARowKeyParser dfaParser;

  private int currentEntry = 0;

  public MultiEntryHBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry[] config) {
    this.context = context;
    this.entries = config;
  }

  private void initConfig() throws Exception {
    this.startRowKey = appendBytes(HBaseRecordReader.parseRkStr(entries[0].getStartRowKey()), produceTail(true));
    this.endRowKey = appendBytes(HBaseRecordReader.parseRkStr(entries[entries.length - 1].getEndRowKey()), produceTail(false));
    this.tableName = entries[0].getTableName();
    this.entryKeys = new Pair[entries.length];
    this.entryFilters = new ArrayList<>();
    this.projections = new ArrayList<>();
    this.entryProjections = new ArrayList<>();
    this.fieldInfoMap = new HashMap<>();

    List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, null);
    for (HBaseFieldInfo col : cols) {
      fieldInfoMap.put(col.fieldSchema.getName(), col);
    }
    for (int i = 0; i < entries.length; i++) {
      entryKeys[i] = new Pair<>(ByteUtils.toBytesBinary(entries[i].getStartRowKey()),ByteUtils.toBytesBinary(entries[i].getEndRowKey())) ;
      this.entryFilters.add(entries[i].getFilters());
      List<NamedExpression> exprs = entries[i].getProjections();
      NamedExpression[] exprArr = new NamedExpression[exprs.size()];
      HBaseFieldInfo[] infos = new HBaseFieldInfo[exprs.size()];
      for (int j = 0; j < exprs.size(); j++) {
        exprArr[j] = exprs.get(j);
        infos[j] = fieldInfoMap.get(((SchemaPath) exprArr[j].getExpr()).getPath().toString());
        if (!projections.contains(exprArr[j]))
          projections.add(exprArr[j]);
        if (false == parseRk && infos[j].fieldType == HBaseFieldInfo.FieldType.rowkey)
          parseRk = true;
      }
      entryProjections.add(exprArr);
      entryProjFieldInfos.add(infos);
    }
    primaryRowKeyParts = TableInfo.getRowKey(tableName, null);
    dfaParser = new DFARowKeyParser(primaryRowKeyParts, fieldInfoMap);
  }

  private void initDirectScanner() throws IOException {
    FilterList filterList = new FilterList();
    long startVersion = Long.MIN_VALUE;
    long stopVersion = Long.MAX_VALUE;
    List<String> patterns = null;
    for (List<HbaseScanPOP.RowkeyFilterEntry> filters : entryFilters) {
      if (filters != null) {
        for (HbaseScanPOP.RowkeyFilterEntry entry : filters) {
          SchemaPath type = entry.getFilterType();
          switch (type.getPath().toString()) {
            case "XARowKeyPatternFilter":
              if (null == patterns)
                patterns = new ArrayList<>();
              for (LogicalExpression e : entry.getFilterExpressions()) {
                String pattern = ((SchemaPath) e).getPath().toString();
                if (patterns.contains(pattern))
                  patterns.add(pattern);
              }
              break;
            case "HbaseFilter":
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
                      SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Bytes.toBytes(cfName),
                        Bytes.toBytes(cqName), op,
                        new BinaryComparator(Bytes
                          .toBytes(
                            rightField
                              .getLong())));
                      filterList.addFilter(valueFilter);
                      break;
                    case cversion:
                      switch (op) {
                        case GREATER:
                          startVersion = rightField.getLong() + 1;
                          break;
                        case GREATER_OR_EQUAL:
                          startVersion = rightField.getLong();
                          break;
                        case LESS:
                          stopVersion = rightField.getLong();
                          break;
                        case LESS_OR_EQUAL:
                          stopVersion = rightField.getLong() + 1;
                          break;
                        case EQUAL:
                          List<Long> timestamps = new ArrayList<>();
                          timestamps.add(rightField.getLong());
                          Filter timeStampsFilter = new TimestampsFilter(timestamps);
                          filterList.addFilter(timeStampsFilter);
                          break;
                      }
                      break;
                    case cqname:
                      Filter qualifierFilter = new QualifierFilter(op, new BinaryComparator(
                        Bytes.toBytes(rightField.getLong())));
                      filterList.addFilter(qualifierFilter);
                    default:
                      break;
                  }

                }
              }
              break;
          }
        }
      }
    }
    scanner = new DirectScanner(startRowKey, endRowKey, tableName, null, false, false);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    try {
      initConfig();
      initDirectScanner();
      setupEntry(currentEntry);
    } catch (Exception e) {
      e.printStackTrace();
      throw new ExecutionSetupException("MultiEntryHbaseRecordReader");
    }
  }

  private void setupEntry(int index) throws SchemaChangeException {
    HBaseFieldInfo[] infos = entryProjFieldInfos.get(index);
    valueVectors = new ArrayList<>(infos.length);
    for (int j = 0; j < infos.length; j++) {
      TypeProtos.MajorType type = HBaseRecordReader.getMajorType(infos[j]);
      ValueVector v = getVector(infos[j].fieldSchema.getName(), type);
      valueVectors.add(v);
      outputMutator.addField(v);
    }
    entryIndexVector = getVector(UnionedScanBatch.UNION_MARKER_VECTOR_NAME, Types.required(TypeProtos.MinorType.INT));
    outputMutator.addField(entryIndexVector);
    outputMutator.setNewSchema();
  }

  private void releaseEntry() {
    for (int i = 0; i < valueVectors.size(); i++) {
      ValueVector v = valueVectors.get(i);
      cleanupVector(v);
    }
    valueVectors.clear();
    if (entryIndexVector != null) {
      cleanupVector(entryIndexVector);
      entryIndexVector = null;
    }
  }

  private void cleanupVector(ValueVector v) {
    logger.debug("removing {}", v.getField());
    try {
      outputMutator.removeField(v.getField());
    } catch (SchemaChangeException e) {
      logger.info("closing vectors failed", e);
    }
    v.close();
  }

  private ValueVector getVector(String name, TypeProtos.MajorType type) {
    if (type.getMode() != TypeProtos.DataMode.REQUIRED)
      throw new UnsupportedOperationException();
    MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
    if (context == null)
      return TypeHelper.getNewVector(f, new DirectBufferAllocator());
    return TypeHelper.getNewVector(f, context.getAllocator());
  }

  public int next() {
    try {
      if (newEntry) {
        releaseEntry();
        setupEntry(currentEntry);
        newEntry = false;
      }
      allocateNew();
      int recordSetIndex = 0;
      while (true) {
        if (valIndex < curRes.size() - 1) {
          int readerEntry = getEntryIndex(curRes.get(valIndex));
          if(readerEntry != currentEntry){
            newEntry = true ;
            return endNext(recordSetIndex + 1, currentEntry, readerEntry);
          }
          int length = splitKeyValues(curRes, valIndex, batchSize - recordSetIndex - 1);
          setValues(curRes, valIndex, length, recordSetIndex);
          recordSetIndex += length;
          if (length + valIndex != curRes.size() - 1) {
            valIndex ++ ;
            return endNext(recordSetIndex + 1,currentEntry,currentEntry);
          } else {
            valIndex = 0;
            curRes.clear();
          }
        }
        if (!scanner.next(curRes)) {
          valIndex = 0 ;
          return endNext(recordSetIndex + 1,currentEntry,currentEntry);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new DrillRuntimeException("Scan failed");
    }
  }

  private int endNext(int valueCount,int entryIndex,int nextEntry ){
    setValueCount(valueCount);
    entryIndexVector.getMutator().setObject(0,entryIndex);
    currentEntry = nextEntry;
    return valueCount;
  }

  private int splitKeyValues(List<KeyValue> keyValues, int offset, int maxSize) {
    int length = Math.min(maxSize, keyValues.size() - 1 - offset );
    int lastEntry = getEntryIndex(keyValues.get(offset + length));
    if (lastEntry != currentEntry) {
      for (int i = offset + length; i >= offset; i++) {
        if (currentEntry == getEntryIndex(keyValues.get(i)))
          return i - offset;
      }
    }
    return length;
  }

  private void setValues(List<KeyValue> keyValues, int offset, int length, int setIndex) {
    for (int i = offset; i < offset + length; i++) {
      setValues(keyValues.get(i), valueVectors, setIndex);
    }
  }

  private void allocateNew() {
    for (ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, batchSize, 8);
    }
    AllocationHelper.allocate(entryIndexVector, batchSize, 4);
  }

  private int getEntryIndex(KeyValue kv) {
    byte[] rk = kv.getRow();
    int i;
    for (i = currentEntry; i < entries.length; i++) {
      if (Bytes.compareTo(rk, entryKeys[i].getFirst()) >= 0 && Bytes.compareTo(rk, entryKeys[i].getSecond()) <= 0)
        return i;
    }
    return currentEntry;
  }

  public static byte[] appendBytes(byte[] orig, byte[] tail) {
    byte[] result = new byte[orig.length + tail.length];
    for (int i = 0; i < result.length; i++) {
      if (i < orig.length)
        result[i] = orig[i];
      else
        result[i] = tail[i - orig.length];
    }
    return result;
  }

  public static byte[] produceTail(boolean start) {
    byte[] result = new byte[7];
    if (start)
      result[0] = '.';
    else
      result[0] = -1;
    result[1] = -1;
    for (int i = 2; i < result.length; i++) {
      if (start)
        result[i] = 0;
      else
        result[i] = -1;
    }
    return result;
  }

  public boolean setValues(KeyValue kv, List<ValueVector> valueVectors, int index) {
    boolean next = true;
    Map<String, Object> rkObjectMap = new HashMap<>();
    if (parseRk)
      rkObjectMap = dfaParser.parse(kv.getRow());
    HBaseFieldInfo[] infos = entryProjFieldInfos.get(currentEntry);
    for (int i = 0; i < infos.length; i++) {
      HBaseFieldInfo info = infos[i];
      ValueVector valueVector = valueVectors.get(i);
      Object result = HBaseRecordReader.getValFromKeyValue(kv, info, rkObjectMap);
      String type = info.fieldSchema.getType();
      if (type.equals("string"))
        result = Bytes.toBytes((String) result);
      valueVector.getMutator().setObject(index, result);
      if (valueVector.getValueCapacity() - index == 1) {
        next = false;
      }
    }
    return next;
  }

  private void setValueCount(int valueCount) {
    for (int i = 0; i < valueVectors.size(); i++) {
      ValueVector v = valueVectors.get(i);
      v.getMutator().setValueCount(valueCount);
    }
    entryIndexVector.getMutator().setValueCount(valueCount);
  }

  @Override
  public void cleanup() {
    try {
      scanner.close();
    } catch (IOException e) {
      logger.info("closing scanner failed", e);
    }
    releaseEntry();
  }

}
