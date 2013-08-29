package org.apache.drill.exec.physical.impl.unionedscan;

import com.xingcloud.hbase.util.DFARowKeyParser;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import com.xingcloud.xa.hbase.filter.XARowKeyPatternFilter;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultiEntryHBaseRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);
  private HbaseScanPOP.HbaseScanEntry[] entries;

  private FragmentContext context;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  private List<List<HbaseScanPOP.RowkeyFilterEntry>> entryFilters;

  private int entryIndex;
  private ValueVector entryIndexVector;
  private List<NamedExpression> projections;
  private List<NamedExpression[]> entryProjections;
  private List<HBaseFieldInfo[]> entryProjFieldInfos = new ArrayList<>();
  private Map<String, HBaseFieldInfo> fieldInfoMap;
  private ValueVector[] valueVectors;
  private OutputMutator outputMutator;
  private boolean parseRk = false;
  private boolean init = false;
  private int batchSize = 1024;

  private boolean newEntry = false;

  private List<DirectScanner> scanners;
  private int currentScannerIndex = 0;
  private int valIndex = -1;
  private boolean hasMore;
  private List<KeyValue> curRes = new ArrayList<>();
  private List<KeyPart> primaryRowKeyParts;
  private DFARowKeyParser dfaParser;

  public MultiEntryHBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry[] config) {
    this.context = context;
    this.entries = config;
  }

  private void initConfig() throws Exception {
    this.startRowKey = HBaseRecordReader.parseRkStr(entries[0].getStartRowKey());
    this.endRowKey = HBaseRecordReader.parseRkStr(entries[entries.length - 1].getEndRowKey());
    this.tableName = entries[0].getTableName();
    this.entryFilters = new ArrayList<>();
    this.entryIndex = 0;
    this.projections = new ArrayList<>();
    this.entryProjections = new ArrayList<>();
    this.fieldInfoMap = new HashMap<>();

    List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, null);
    for (HBaseFieldInfo col : cols) {
      fieldInfoMap.put(col.fieldSchema.getName(), col);
    }
    for (int i = 0; i < entries.length; i++) {
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

  private void initDirectScanner() throws IOException{
    this.scanners = new ArrayList<>();
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
    scanners.add(new DirectScanner(startRowKey, endRowKey, tableName, null, false, false));
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    try {
      initConfig();
      initDirectScanner();
      entryIndexVector = getVector(UnionedScanBatch.UNION_MARKER_VECTOR_NAME, Types.required(TypeProtos.MinorType.INT));
      setupEntry(entryIndex);
    } catch (Exception e) {
      throw new ExecutionSetupException("MultiEntryHbaseRecordReader");
    }
  }

  private void setupEntry(int index) throws SchemaChangeException {
      HBaseFieldInfo[] infos=entryProjFieldInfos.get(index);
      valueVectors = new ValueVector[infos.length];
      for(int j=0;j<infos.length;j++){
          TypeProtos.MajorType type = HBaseRecordReader.getMajorType(infos[j]);
          valueVectors[j] = getVector(infos[j].fieldSchema.getName(),type);
          outputMutator.addField(valueVectors[j]);
      }
      outputMutator.addField(entryIndexVector);
      outputMutator.setNewSchema();
  }

  private void realeaseEntry() {
    try {
      for (int i = 0; i < valueVectors.length; i++) {
        outputMutator.removeField(valueVectors[i].getField());
        valueVectors[i].close();
      }
      outputMutator.removeField(entryIndexVector.getField());
      entryIndexVector.close();
    } catch (Exception e) {

    }
  }

  private ValueVector getVector(String name, TypeProtos.MajorType type) {
    if (type.getMode() != TypeProtos.DataMode.REQUIRED)
      throw new UnsupportedOperationException();
    MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
    if (context == null)
      return TypeHelper.getNewVector(f, new DirectBufferAllocator());
    return TypeHelper.getNewVector(f, context.getAllocator());
  }

  @Override
  public int next() {
    if (newEntry) {
      realeaseEntry();
      try {
        setupEntry(entryIndex);
      } catch (SchemaChangeException e) {
        throw new IllegalArgumentException(e);
      }
      newEntry = false;
    }

    for (ValueVector v : valueVectors) {
      AllocationHelper.allocate(v, batchSize, 8);
    }
    AllocationHelper.allocate(entryIndexVector, batchSize, 4);
    int recordSetIndex = 0;
    int nextEntryIndex;
    while (true) {
      if (currentScannerIndex > scanners.size() - 1) {
        setValueCount(recordSetIndex);
        return recordSetIndex;
      }
      DirectScanner scanner = scanners.get(currentScannerIndex);
      if (valIndex == -1) {
        if (scanner == null) {
          return 0;
        }
        try {
          hasMore = scanner.next(curRes);
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
            hasMore = scanner.next(curRes);
          } catch (IOException e) {
            e.printStackTrace();
          }
          valIndex = 0;
          if (!hasMore)
            currentScannerIndex++;
          if (curRes.size() != 0) {
            KeyValue kv = curRes.get(valIndex++);
            nextEntryIndex = getEntryIndex(kv);
            if (nextEntryIndex != entryIndex) {
              valIndex--;
              entryIndex = nextEntryIndex;
              newEntry = true;
              return recordSetIndex;
            }
            boolean next = setValues(kv, valueVectors, recordSetIndex);
            entryIndexVector.getMutator().setObject(recordSetIndex, entryIndex);
            recordSetIndex++;
            if (!next) {
              setValueCount(recordSetIndex);
              return recordSetIndex;

            }
            break;
          }
        }
        if (valIndex > curRes.size() - 1) {
          if (!hasMore)
            valIndex = -1;
          continue;
        }

      }
      KeyValue kv = curRes.get(valIndex++);
      nextEntryIndex = getEntryIndex(kv);
      if (nextEntryIndex != entryIndex) {
        valIndex--;
        entryIndex = nextEntryIndex;
        newEntry = true;
        return recordSetIndex;
      }
      boolean next = setValues(kv, valueVectors, recordSetIndex);
      entryIndexVector.getMutator().setObject(recordSetIndex, entryIndex);
      recordSetIndex++;
      if (!next) {
        setValueCount(recordSetIndex);
        return recordSetIndex;
      }
    }
  }

  private int getEntryIndex(KeyValue kv) {
    byte[] rk = kv.getRow();
    int i;
    for (i = entryIndex; i < entries.length; i++) {
      byte[] currentSrk = ByteUtils.toBytesBinary(entries[i].getStartRowKey());
      byte[] currentEnk = ByteUtils.toBytesBinary(entries[i].getEndRowKey());
      if (Bytes.compareTo(rk, currentSrk) > 0 && Bytes.compareTo(rk, currentEnk) < 0)
        return i;
    }
    return entryIndex;

  }

  public boolean setValues(KeyValue kv, ValueVector[] valueVectors, int index) {
    boolean next = true;
    Map<String, Object> rkObjectMap = new HashMap<>();
    if (parseRk)
      rkObjectMap = dfaParser.parse(kv.getRow());
    HBaseFieldInfo[] infos = entryProjFieldInfos.get(entryIndex);
    for (int i = 0; i < infos.length; i++) {
      HBaseFieldInfo info = infos[i];
      ValueVector valueVector = valueVectors[i];
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
    for (int i = 0; i < valueVectors.length; i++) {
      valueVectors[i].getMutator().setValueCount(valueCount);
    }
    entryIndexVector.getMutator().setValueCount(valueCount);
  }

  @Override
  public void cleanup() {
    try {
      for (int i = 0; i < valueVectors.length; i++) {
        outputMutator.removeField(valueVectors[i].getField());
        valueVectors[i].close();
      }
      for (DirectScanner scanner : scanners) {
        scanner.close();
      }
    } catch (SchemaChangeException | IOException e) {
      e.printStackTrace();
    }
  }
}
