package org.apache.drill.exec.store;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseUserScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarChar4Vector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.TableScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

//import javax.security.auth.login.Configuration;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/17/13
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseUserRecordReader implements RecordReader {
  private ValueVector[] valueVectors;
  private String property;
  private String val;
  private FragmentContext context;
  private OutputMutator output;
  private int batch = 1024;
  private Map<String, MinorType> typeMap;
  private String project_id;
  private String property_type;
  private int property_id;

  private List<TableScanner> scanners = new ArrayList<>();

  private int currentScannerIndex = 0;
  private List<KeyValue> curRes = new ArrayList<KeyValue>();
  private int valIndex = -1;
  private boolean hasMore;
  private boolean init = false;

  HbaseUserScanPOP.HbaseUserScanEntry config;

  public HBaseUserRecordReader(FragmentContext context, HbaseUserScanPOP.HbaseUserScanEntry config) {
    this.config = config;
    this.property = config.getProperty();
    this.val = config.getvalue();
    this.context = context;
    this.project_id = config.getProject();
    typeMap = new HashMap<>();
    initPropertyTypes();

  }

  private void initUserTableScanner() {
    //String startKey=property_id+getTodayDateStr()+val;

    //String startKey=property_id+"20130619"+val;
    String day = "20130619";
    byte[] srk;
    byte[] enk;
    if (!val.equals("null")) {
      srk = CombineBytes(Bytes.toBytes((short) property_id), Bytes.toBytes(day), Bytes.toBytes(val));
      String nextVal = getNextRkString(val);
      enk = CombineBytes(Bytes.toBytes((short) property_id), Bytes.toBytes(day), Bytes.toBytes(nextVal));
    } else {
      srk = CombineBytes(Bytes.toBytes((short) property_id), Bytes.toBytes(day));
      String nextDay = getNextRkString(day);
      enk = CombineBytes(Bytes.toBytes((short) property_id), Bytes.toBytes(nextDay));
    }
    String tableName = "property_" + project_id + "_index";
    TableScanner scanner = new TableScanner(srk, enk, tableName, null, false, false);
    scanners.add(scanner);
  }

  private byte[] CombineBytes(byte[]... bytesArrays) {
    int length = 0;
    for (byte[] bytes : bytesArrays) {
      length += bytes.length;
    }
    byte retBytes[] = new byte[length];
    int pos = 0;
    for (byte[] bytes : bytesArrays) {
      System.arraycopy(bytes, 0, retBytes, pos, bytes.length);
      pos += bytes.length;
    }
    return retBytes;
  }


  private String getNextRkString(String startKey) {
    StringBuilder b = new StringBuilder(startKey);
    b.setCharAt(startKey.length() - 1, (char) (b.charAt(startKey.length() - 1) + 1));
    return b.toString();
  }

  private String getTodayDateStr() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    Date date = new Date();
    return sdf.format(date);
  }


  private void initPropertyTypes() {
    typeMap.put("sql_bigint", MinorType.BIGINT);
    typeMap.put("sql_datetime", MinorType.BIGINT);
    typeMap.put("sql_string", MinorType.VARCHAR4);

    try {
      Configuration conf = HBaseConfiguration.create();
      HTable table = new HTable(conf, "properties_" + project_id);
      byte[] rk = Bytes.toBytes(property);
      Get get = new Get(rk);
      Result result = table.get(get);
      for (KeyValue kv : result.raw()) {
        if (Bytes.toString(kv.getQualifier()).equals("type"))
          property_type = Bytes.toString(kv.getValue());
        else if (Bytes.toString(kv.getQualifier()).equals("id"))
          property_id = Bytes.toInt(kv.getValue());
      }
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }


  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    valueVectors = new ValueVector[2];

    try {
      MajorType type1 = getMajorType(MinorType.INT);
      valueVectors[0] = getVector(0, "uid", type1, batch);
      output.addField(valueVectors[0]);
      output.setNewSchema();
      MajorType type2 = getMajorType(typeMap.get(property_type));
      valueVectors[1] = getVector(1, property, type2, batch);
      output.addField(valueVectors[1]);
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    //To change body of implemented methods use File | Settings | File Templates.
  }

  private MajorType getMajorType(MinorType type) {
    MajorType.Builder b = MajorType.newBuilder();
    b.setMinorType(type);
    b.setMode(DataMode.REQUIRED);
    return b.build();
  }

  private ValueVector getVector(int fieldId, String name, MajorType type, int length) {

    if (type.getMode() != DataMode.REQUIRED) throw new UnsupportedOperationException();

    MaterializedField f = MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), type);
    ValueVector v;
    BufferAllocator allocator;
    if (context != null) allocator = context.getAllocator();
    else allocator = new DirectBufferAllocator();
    v = TypeHelper.getNewVector(f, allocator);
    if (v instanceof FixedWidthVector) {
      ((FixedWidthVector) v).allocateNew(length);
    } else if (v instanceof VariableWidthVector) {
      ((VariableWidthVector) v).allocateNew(50 * length, length);
    }
    return v;
  }

  @Override
  public int next() {
    if (!init) {
      initUserTableScanner();
      init = true;
    }
    for (ValueVector v : valueVectors) {
      if (v instanceof FixedWidthVector) {
        ((FixedWidthVector) v).allocateNew(batch);
      } else if (v instanceof VariableWidthVector) {
        ((VariableWidthVector) v).allocateNew(50 * batch, batch);
      } else {
        throw new UnsupportedOperationException();
      }
    }
    int recordSetSize = 0;
    while (true) {
      if (currentScannerIndex > scanners.size() - 1) return recordSetSize;
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
            boolean next = PutPropertyValueToVectors(kv, valueVectors, recordSetSize);
            if (!next) return recordSetSize;
            recordSetSize++;
            break;
          }
        }
        if (valIndex > curRes.size() - 1) {
          if (!hasMore) valIndex = -1;
          continue;
        }

      }
      KeyValue kv = curRes.get(valIndex++);
      boolean next = PutPropertyValueToVectors(kv, valueVectors, recordSetSize);
      if (!next) return recordSetSize;
      recordSetSize++;
    }
  }

  private boolean PutPropertyValueToVectors(KeyValue kv, ValueVector[] valueVectors, int recordSetSize) {
    long innerUid = getUidOfLong(kv.getQualifier());
    int uid = (int) (0xffffffff & (long) innerUid);
    IntVector uidVector = (IntVector) valueVectors[0];
    if (recordSetSize > uidVector.getValueCapacity() - 1) return false;
    uidVector.getMutator().set(recordSetSize, uid);
    uidVector.getMutator().setValueCount(recordSetSize);
    byte[] value;
    if ("null".equals(val)) {
      byte[] rk = kv.getRow();
      value = getValueFromRowKey(rk);
    } else
      value = Bytes.toBytes(val);
    if (property_type.equals("sql_string")) {
      VarChar4Vector valueVector = (VarChar4Vector) valueVectors[1];
            /*
            Fixed4 lengthVector=valueVector.getLengthVector();
            int preOffset=0;
            if (recordSetSize != 0) preOffset = lengthVector.getInt(recordSetSize - 1);
            int offset = preOffset + value.length;
            if (offset > (lengthVector.capacity()+1) * 4) return false; */
      valueVector.getMutator().set(recordSetSize, value);
      valueVector.getMutator().setValueCount(recordSetSize);
    } else if (property_type.equals("sql_bigint")) {
      BigIntVector valueVector = (BigIntVector) valueVectors[1];
      valueVector.getMutator().set(recordSetSize, (long) Long.parseLong(val));
      valueVector.getMutator().setValueCount(recordSetSize);
      if (recordSetSize + 2 > valueVector.getValueCapacity()) return false;
    } else {
      System.out.println("error property_type " + property_type);
      return false;
    }
    return true;
  }

  private byte[] getValueFromRowKey(byte[] rk) {
    int length = rk.length;
    byte[] value = new byte[length - 10];
    int j = 0;
    for (int i = 10; i < length; i++) {
      value[j++] = rk[i];
    }
    return value;
  }

  private long getUidOfLong(byte[] rawUid) {
    byte[] uid = new byte[8];
    int length = rawUid.length;
    int i = 0;
    for (; i < 8 - length; i++) {
      uid[i] = 0;
    }

    for (int j = 0; j < rawUid.length; j++) {
      uid[i++] = rawUid[j];
    }
    return Bytes.toLong(uid);
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
