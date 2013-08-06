package org.apache.drill.exec.store;

import com.sun.jersey.core.util.Base64;
import com.xingcloud.hbase.util.DFARowKeyParser;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.TableScanner;
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
  static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

  private HbaseScanPOP.HbaseScanEntry config;
  private FragmentContext context;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;

  private List<HBaseFieldInfo> projections;
  private Map<String, String> sourceRefMap;
  private List<LogicalExpression> filters;
  private List<HBaseFieldInfo> primaryRowKey;
  private List<KeyPart> primaryRowKeyParts;
  private Map<String, HBaseFieldInfo> fieldInfoMap;
  private DFARowKeyParser dfaParser;

  private List<TableScanner> scanners = new ArrayList<>();
  private int currentScannerIndex = 0;
  private List<KeyValue> curRes = new ArrayList<KeyValue>();
  private int valIndex = -1;
  private boolean hasMore;
  private int BATCHRECORDCOUNT = 1024 * 4;
  private ValueVector[] valueVectors;
  private boolean init = false;

  private Map<Object,String> testMap=new HashMap<>();


  public HBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry config) {
    this.context = context;
    this.config = config;
    initConfig();
  }

  private void initConfig() {
    startRowKey = parseRkStr(config.getStartRowKey());
    endRowKey = parseRkStr(config.getEndRowKey());
    tableName = config.getTableName();
    projections = new ArrayList<>();
    fieldInfoMap = new HashMap<>();
    sourceRefMap = new HashMap<>();
    List<NamedExpression> logProjection = config.getProjections();
    List<String> options = new ArrayList<>();
    for (int i = 0; i < logProjection.size(); i++) {
      options.add((String) ((SchemaPath) logProjection.get(i).getExpr()).getPath());
    }
    try {
      List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, options);
      for (HBaseFieldInfo col : cols) {
        fieldInfoMap.put(col.fieldSchema.getName(), col);
      }

      for (NamedExpression e : logProjection) {
        String ref = (String) e.getRef().getPath();
        String name = (String) ((SchemaPath) e.getExpr()).getPath();
        sourceRefMap.put(name, ref);
        if (!fieldInfoMap.containsKey(name)) {
          LOG.debug("wrong field " + name + " hbase table has no this field");
        } else {
          projections.add(fieldInfoMap.get(name));
        }
      }
      filters = config.getFilters();

      primaryRowKeyParts = TableInfo.getRowKey(tableName, options);
      primaryRowKey = new ArrayList<>();
      for (KeyPart kp : primaryRowKeyParts) {
        if (kp.getType() == KeyPart.Type.field)
          primaryRowKey.add(fieldInfoMap.get(kp.getField().getName()));
      }
      dfaParser=new DFARowKeyParser(primaryRowKeyParts,fieldInfoMap);
      //primaryRowKey=TableInfo.getPrimaryKey(tableName);

    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  /*
     parse Rk in physical_test: "test"+propId("03")+day("20121201")+[type("str"/"num")+val("en"/"123")]
    */
  private byte[] parseRkStr(String origRk) {
    byte[] result;
    if (origRk.startsWith("test")) {
      String content=origRk.substring(4);
      result=escape(content);
    }
    else {
        result= Base64.decode(origRk);
    }
    return result;
  }

  private static byte[] escape(String constant) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < constant.length(); i++) {
        char c = constant.charAt(i);
        if(c == '\\' && constant.length()>i+3
                && constant.charAt(i+1) =='x'){
            char h = constant.charAt(i+2);
            char l = constant.charAt(i+3);
            baos.write(Integer.parseInt(constant.substring(i+2, i+4), 16));
            i+=3;
        }else{
            baos.write(c);
        }
    }
    return baos.toByteArray();
  }

  /*
  private byte[] addMaxByteToTail(byte[] orig) {
    byte[] result = new byte[orig.length + 1];
    int i = 0;
    for (; i < orig.length; i++) {
      result[i] = orig[i];
    }
    result[i] = (byte)255;
    return result;
  }
  */


  private void initTableScanner() {

    scanners = new ArrayList<>();
    List<Filter> filtersList = new ArrayList<>();
    long startVersion = Long.MIN_VALUE;
    long stopVersion = Long.MAX_VALUE;
    if (filters != null) {
      for (LogicalExpression e : filters) {
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
              filtersList.add(valueFilter);
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
                  filtersList.add(timeStampsFilter);
                  break;
              }
              break;
            case cqname:
              Filter qualifierFilter =
                new QualifierFilter(op, new BinaryComparator(Bytes.toBytes(rightField.getLong())));
              filtersList.add(qualifierFilter);
            default:
              break;
          }

        }
      }
    }
    TableScanner scanner;
    if (startVersion == Long.MIN_VALUE && stopVersion == Long.MAX_VALUE)
      scanner = new TableScanner(startRowKey, endRowKey, tableName, filtersList, false, false);
    else
      scanner = new TableScanner(startRowKey, endRowKey, tableName, filtersList, false, false, startVersion, stopVersion);
    scanners.add(scanner);
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      valueVectors = new ValueVector[projections.size()];
      for (int i = 0; i < projections.size(); i++) {
        MajorType type = getMajorType(projections.get(i));
        int batchRecordCount = BATCHRECORDCOUNT;
        valueVectors[i] =
          getVector(i, sourceRefMap.get(projections.get(i).fieldSchema.getName()), type, batchRecordCount);
        output.addField(valueVectors[i]);
        output.setNewSchema();
      }
    } catch (Exception e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
  }

  private MajorType getMajorType(HBaseFieldInfo info) {
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

  private ValueVector getVector(int fieldId, String name, MajorType type, int length) {

    if (type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();

    MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
    ValueVector v;
    BufferAllocator allocator;
    if (context != null) allocator = context.getAllocator();
    else allocator = new DirectBufferAllocator();
    v = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(v, length, 50);
    return v;

  }


  @Override
  public int next() {
    if (!init) {
      initTableScanner();
      init = true;
    }
    for (ValueVector v : valueVectors) {
      if (v instanceof FixedWidthVector) {
        ((FixedWidthVector) v).allocateNew(BATCHRECORDCOUNT);
      } else if (v instanceof VariableWidthVector) {
        ((VariableWidthVector) v).allocateNew(50 * BATCHRECORDCOUNT, BATCHRECORDCOUNT);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    int recordSetIndex = 0;
    while (true) {
      if (currentScannerIndex > scanners.size() - 1)
      {
        setValueCount(recordSetIndex);
        return recordSetIndex;
      }
      TableScanner scanner = scanners.get(currentScannerIndex);
      if (valIndex == -1) {
        if (scanner == null) {
          return 0;
        }
        try {
          hasMore = scanner.next(curRes);

        } catch (IOException e) {
          e.printStackTrace();
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
          if (!hasMore) currentScannerIndex++;
          if (curRes.size() != 0) {
            KeyValue kv = curRes.get(valIndex++);
            boolean next = setValues(kv, valueVectors, recordSetIndex);
            recordSetIndex++;
            if (!next) {
              setValueCount(recordSetIndex);
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
        return recordSetIndex;
      }

    }
  }

  public boolean setValues(KeyValue kv, ValueVector[] valueVectors, int index) {
    boolean next = true;
    Map<String, Object> rkObjectMap = dfaParser.parse(kv.getRow());
    for (int i = 0; i < projections.size(); i++) {
      HBaseFieldInfo info = projections.get(i);
      ValueVector valueVector = valueVectors[i];
      Object result = getValFromKeyValue(kv, info, rkObjectMap);
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
  }

  public Object getValFromKeyValue(KeyValue keyvalue, HBaseFieldInfo option, Map<String, Object> rkObjectMap) {
    String fieldName = option.fieldSchema.getName();
    if (option.fieldType == HBaseFieldInfo.FieldType.rowkey) {
      if (!rkObjectMap.containsKey(fieldName))
        LOG.info("error! " + fieldName + " does not exists in this keyvalue");
      else
        return rkObjectMap.get(fieldName);
    } else if (option.fieldType == HBaseFieldInfo.FieldType.cellvalue) {
      String cfName = Bytes.toString(keyvalue.getFamily());
      String cqName = Bytes.toString(keyvalue.getQualifier());
      if (!option.cfName.equals(cfName) || !option.cqName.equals(cqName))
        LOG.info("error! this field's column info---" + option.cqName + ":" + option.cqName +
          " does not match the keyvalue's column info---" + cfName + ":" + cqName);
      else {
        return DFARowKeyParser.parseBytes(keyvalue.getValue(),option.fieldSchema.getType());
      }
    } else if (option.fieldType == HBaseFieldInfo.FieldType.cversion) {
      return keyvalue.getTimestamp();
    } else if (option.fieldType == HBaseFieldInfo.FieldType.cqname) {
      byte[] qualif=keyvalue.getQualifier();
      byte[] orig=new byte[4];
      for(int i=0;i<4;i++)
          orig[i]=keyvalue.getQualifier()[i+1];
      return DFARowKeyParser.parseBytes(orig,option.fieldSchema.getType());
    }
    return null;
  }


  @Override
  public void cleanup() {
    for (TableScanner scanner : scanners) {
      try {
        scanner.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }


}

