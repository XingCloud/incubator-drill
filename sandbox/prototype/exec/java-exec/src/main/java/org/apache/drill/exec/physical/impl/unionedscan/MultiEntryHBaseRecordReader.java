package org.apache.drill.exec.physical.impl.unionedscan;

import com.xingcloud.hbase.util.Constants;
import com.xingcloud.hbase.util.RowKeyUtils;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.filter.XARkConditionFilter;
import com.xingcloud.xa.hbase.filter.XARkConditionFilter.*;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.util.EventTableUtil;
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
import org.apache.drill.exec.util.parser.DFARowKeyParser;
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
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.regionserver.HBaseClientScanner;
import org.apache.hadoop.hbase.regionserver.XAScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

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

  private List<HBaseFieldInfo[]> entryProjFieldInfos = new ArrayList<>();
  //记录row key中的投影
  private List<Map<String, HBaseFieldInfo>> entriesRowKeyProjs = new ArrayList<>();
  //记录在family, qualifier, value, ts中所需要的投影
  private List<Map<String, HBaseFieldInfo>> entriesOtherProjs = new ArrayList<>();

  private Map<String, HBaseFieldInfo> fieldInfoMap;

  private List<ValueVector> valueVectors;

  //记录每个投影的value vector所对应的在valueVectors中的位置
  private Map<String, ValueVector> vvMap;

  private OutputMutator outputMutator;
  private int batchSize = 1024 * 63;

  private boolean newEntry = false;

  private XAScanner scanner;
  private int valIndex = 0;
  private List<KeyValue> curRes = new ArrayList<>();
  private List<KeyPart> primaryRowKeyParts;

  private DFARowKeyParser dfaParser;
  private List<Boolean> useDFA = new ArrayList<>();

  private int currentEntry = 0;
  private int nextEntry = 1;

  private long timeCost = 0;
  private long start = 0;

  private boolean hasMore = true;

  private Pair<byte[], byte[]> uidRange = new Pair<>();

  public MultiEntryHBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry[] config) {
    this.context = context;
    this.entries = config;
  }

  private void initConfig() throws Exception {
    //起始和结束hbase的start key和end key
    this.startRowKey = entries[0].getStartRowKey();
    this.endRowKey = entries[entries.length - 1].getEndRowKey();
    //起始和结束uid的限制（hbase的最后5字节）
    uidRange.setFirst(entries[0].getStartUid());
    uidRange.setSecond(entries[0].getEndUid());

    this.tableName = entries[0].getTableName();
    this.entryKeys = new Pair[entries.length];
    this.entryFilters = new ArrayList<>();
    this.fieldInfoMap = new HashMap<>();

    logger.info("Init config in MultiEntryHBaseRecordReader. Start key: " + Bytes.toStringBinary(startRowKey) +
      "---End key: " + Bytes.toStringBinary(endRowKey) + "\tStart uid: " + Bytes.toStringBinary(uidRange.getFirst()) + "---End uid: " +
      Bytes.toStringBinary(uidRange.getSecond()));

    for (int i=0; i<entries.length; i++) {
      useDFA.add(false);
    }

    try{
    List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, null);
    for (HBaseFieldInfo col : cols) {
      fieldInfoMap.put(col.fieldSchema.getName(), col);
    }
    for (int i = 0; i < entries.length; i++) {
      entryKeys[i] = new Pair<>(entries[i].getStartRowKey(),entries[i].getEndRowKey()) ;
      this.entryFilters.add(entries[i].getFilters());

      entries[i].setFilters(null);

      List<NamedExpression> exprs = entries[i].getProjections();
      NamedExpression[] exprArr = new NamedExpression[exprs.size()];
      HBaseFieldInfo[] infos = new HBaseFieldInfo[exprs.size()];
      Map<String, HBaseFieldInfo> rkProjs = new HashMap<>();
      Map<String, HBaseFieldInfo> otherProjs = new HashMap<>();
      for (int j = 0; j < exprs.size(); j++) {
        exprArr[j] = exprs.get(j);
        try{
           infos[j] = fieldInfoMap.get( ((SchemaPath)exprArr[j].getExpr()).getPath().toString());
        }catch (Exception e){
            logger.info(" error !"+ exprArr[j].getExpr().toString()+ " is not schemaPath");
            e.printStackTrace();
            throw e;
        }

        if (infos[j].fieldType == HBaseFieldInfo.FieldType.rowkey) {
          rkProjs.put(infos[j].fieldSchema.getName(), infos[j]);
          if (infos[j].serLength <= 0) {
            logger.debug("Need DFA parser to parse " + infos[j].fieldSchema.getName());
            useDFA.set(i, true);
          }
        } else {
          otherProjs.put(infos[j].fieldSchema.getName(), infos[j]);
        }
      }
      entriesRowKeyProjs.add(rkProjs);
      entriesOtherProjs.add(otherProjs);
      entryProjFieldInfos.add(infos);
    }
    primaryRowKeyParts = TableInfo.getRowKey(tableName, null);
    dfaParser = new DFARowKeyParser(primaryRowKeyParts, fieldInfoMap);


    }catch (Exception e){
        e.printStackTrace();
        throw e;
    }
  }

  private void initDirectScanner() throws IOException {
    long initStart = System.nanoTime() ;
    FilterList filterList = new FilterList();
    Set<String> patterns = new HashSet<>();
    List<KeyRange> slot = new ArrayList<>();
    for(int i=0; i<entryFilters.size(); i++){
      List<HbaseScanPOP.RowkeyFilterEntry> filters = entryFilters.get(i);
      if(filters == null || filters.size() == 0){
        KeyRange range = new KeyRange(entries[i].getStartRowKey(), true,
                entries[i].getEndRowKey(), false);
        slot.add(range);
        logger.debug("Add key range: " + range);
      }
      else {
        for (HbaseScanPOP.RowkeyFilterEntry entry : filters) {
          Constants.FilterType type = entry.getFilterType();
          switch (type) {
            case XaRowKeyPattern:
              for (String pattern : entry.getFilterExpressions()) {
                if (!patterns.contains(pattern)){
                  patterns.add(pattern);
                }
              }
              break;
            case HbaseOrig:
              RowKeyRange range=new RowKeyRange(entries[i].getStartRowKey(),entries[i].getEndRowKey());
              for (String filterExpr : entry.getFilterExpressions()) {
                LogicalExpression e=
                  context.getDrillbitContext().getConfig().getMapper().readValue(filterExpr,LogicalExpression.class);
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
                      XARkConditionFilter conditionValueFilter=new XARkConditionFilter(range,valueFilter);
                      filterList.addFilter(conditionValueFilter);
                      break;
                    case cqname:
                      Filter qualifierFilter = new QualifierFilter(op, new BinaryComparator(
                        Bytes.toBytes(rightField.getLong())));
                      XARkConditionFilter conditionQfFilter=new XARkConditionFilter(range,qualifierFilter);
                      filterList.addFilter(conditionQfFilter);
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

    //Release JVM memory
    entryFilters = null;
    if(patterns.size() > 0 || slot.size() > 0) {  //todo should depend on hbase schema to generate row key
      if (patterns.size() > 0) {

        for (String event : patterns) {
          byte[] eventBytes = Bytes.toBytesBinary(event);
          byte[] lowerRange = Bytes.add(eventBytes, RowKeyUtils.produceTail(true));
          byte[] upperRange = Bytes.add(eventBytes, RowKeyUtils.produceTail(false));
          KeyRange keyRange = new KeyRange(lowerRange, true, upperRange, true);
          logger.debug("Add Key range: " + keyRange);
          slot.add(keyRange);
        }
      }
      patterns = null;
      Collections.sort(slot,keyRangeComparator);
      Filter skipScanFilter = new SkipScanFilter(slot, uidRange);
      filterList.addFilter(skipScanFilter);
    }
    scanner = new DirectScanner(startRowKey, endRowKey, tableName, filterList, false, false);
    StringBuilder summary = new StringBuilder("Start key: " + Bytes.toStringBinary(startRowKey) +
            "\tEnd key: " + Bytes.toStringBinary(endRowKey) +
            "\nStart uid: " + Bytes.toStringBinary(uidRange.getFirst()) + "\tEnd uid: " + Bytes.toStringBinary(uidRange.getSecond())
                    + "\nKey range size: " + slot.size()+"\n");
    for (KeyRange range : slot) {
      summary.append(range).append("\n");
    }
    logger.info(summary.toString());

    //test
    //scanner= new HBaseClientScanner(startRowKey,endRowKey,tableName,filterList);
    logger.info("Init scanner cost {} mills .",(System.nanoTime() - initStart)/1000000);
  }

  private static Comparator keyRangeComparator = new Comparator() {
        @Override
        public int compare(Object o1, Object o2) {
            KeyRange range1 = (KeyRange)o1;
            byte[] lowerRange1 = range1.getLowerRange();
            KeyRange range2 = (KeyRange)o2;
            byte[] lowerRange2 = range2.getLowerRange();
            return Bytes.compareTo(lowerRange1, lowerRange2);
        }
  };


    @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    try {
      initConfig();
      setupEntry(currentEntry);
    } catch (Exception e) {
      e.printStackTrace();
      throw new ExecutionSetupException("MultiEntryHbaseRecordReader");
    }
  }

  private void setupEntry(int index) throws SchemaChangeException {
    HBaseFieldInfo[] infos = entryProjFieldInfos.get(index);
    valueVectors = new ArrayList<>(infos.length);
    vvMap = new HashMap<>(valueVectors.size());

    for (int j = 0; j < infos.length; j++) {
      TypeProtos.MajorType type = HBaseRecordReader.getMajorType(infos[j]);
      ValueVector v = getVector(infos[j].fieldSchema.getName(), type);
      valueVectors.add(v);
      outputMutator.addField(v);
      vvMap.put(infos[j].fieldSchema.getName(), v);
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
    if(scanner == null){
      try{
        initDirectScanner();
      } catch (Exception e){
        e.printStackTrace();
        throw new DrillRuntimeException("Init scanner failed .",e) ;
      }
    }
    start = System.currentTimeMillis();
    try {
      if (newEntry) setUpNewEntry();
      allocateNew();
      int recordSetIndex = 0;
      while (true) {
        if (valIndex < curRes.size()) {
          int readerEntry = getEntryIndex(curRes.get(valIndex));
          if(readerEntry != currentEntry){
            nextEntry = readerEntry;
            newEntry = true ;
            if(recordSetIndex == 0){
              setUpNewEntry();
              allocateNew();
              continue;
            }
            return endNext(recordSetIndex);
          }
          int length = splitKeyValues(curRes, valIndex, batchSize - recordSetIndex);
          setValues(curRes, valIndex, length, recordSetIndex);
          recordSetIndex += length;
          if (length + valIndex != curRes.size()) {
            valIndex += length ;
            return endNext(recordSetIndex);
          } else {
            valIndex = 0;
            curRes.clear();
          }
        }
        if(hasMore){
          hasMore = scanner.next(curRes);
        }
        if (curRes.isEmpty()) {
          hasMore = false ;
          valIndex = 0 ;
          return endNext(recordSetIndex);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new DrillRuntimeException("Scan failed", e);
    }
  }

  private void setUpNewEntry() throws SchemaChangeException{
    releaseEntry();
    if(currentEntry >= nextEntry){
      logger.error("Overlap {} {}",currentEntry,nextEntry);
      throw new DrillRuntimeException("Overlap") ;
    }
    currentEntry = nextEntry ;
    setupEntry(currentEntry);
    newEntry = false;
  }

  private int endNext(int valueCount){
    timeCost += System.currentTimeMillis() - start ;
    if(valueCount == 0)
      return 0;
    setValueCount(valueCount);
    entryIndexVector.getMutator().setObject(0,currentEntry);
    return valueCount;
  }

  private int splitKeyValues(List<KeyValue> keyValues, int offset, int maxSize) {
    int length = Math.min(maxSize, keyValues.size()  - offset );
    if(length == 0){
      return  0;
    }
    int lastEntry = getEntryIndex(keyValues.get(offset + length - 1));
    if (lastEntry != currentEntry) {
      for (int i = offset + length - 1; i >= offset; i--) {
        if (currentEntry == getEntryIndex(keyValues.get(i)))
          return i - offset + 1;
      }
      return 0;
    }
    return length;
  }

  private void setValues(List<KeyValue> keyValues, int offset, int length, int setIndex) {
    for (int i = offset; i < offset + length; i++) {
      setValues(keyValues.get(i), setIndex);
      setIndex ++ ;
    }
  }

  private void allocateNew() {
    for (ValueVector v : valueVectors) {
      v.clear();
      AllocationHelper.allocate(v, batchSize, 8);
    }
    entryIndexVector.clear();
    AllocationHelper.allocate(entryIndexVector, 1, 4);
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

  public void setValues(KeyValue kv, int index) {
    Map<String, HBaseFieldInfo> rkProjs = entriesRowKeyProjs.get(currentEntry);
    //更新row key里的投影值
    if (rkProjs.size() != 0) {
      dfaParser.parseAndSet(kv.getRow(), rkProjs, vvMap, index, useDFA.get(currentEntry));
    }
    //更新family，qualifier，ts和value的投影值
    Map<String, HBaseFieldInfo> otherProjs = entriesOtherProjs.get(currentEntry);
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
  }

  private void setValueCount(int valueCount) {
    for (int i = 0; i < valueVectors.size(); i++) {
      ValueVector v = valueVectors.get(i);
      v.getMutator().setValueCount(valueCount);
    }
    entryIndexVector.getMutator().setValueCount(1);
  }

  @Override
  public void cleanup() {
    logger.info("MultiEntryHBaseRecordReader finished . ");
    logger.debug("Cost time " + timeCost + "mills");
    try {
      if(scanner != null){
        scanner.close();
        scanner = null ;
      }
    } catch (IOException e) {
      logger.info("closing scanner failed", e);
    }
    releaseEntry();
  }

}
