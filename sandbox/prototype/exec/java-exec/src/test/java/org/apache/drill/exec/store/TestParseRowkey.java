package org.apache.drill.exec.store;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.parser.DFARowKeyParser;

import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.FileSystemNotFoundException;
import java.util.*;
import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/31/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestParseRowkey {
    private static final Log LOG = LogFactory.getLog(TestParseRowkey.class);

    private Map<String, HBaseFieldInfo> rkFieldInfoMap=new HashMap<>();
    private List<KeyPart> primaryRowKeyParts;
    private String tableName;
    private DFARowKeyParser dfaRowKeyParser;

    private static final int BATCH_SIZE = 16 * 1024;


    @Before
    public void init() throws Exception {
        tableName="deu_age";
        List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, null);
        for (HBaseFieldInfo col : cols) {
            rkFieldInfoMap.put(col.fieldSchema.getName(), col);
        }
        primaryRowKeyParts = TableInfo.getRowKey(tableName, null);
        for (KeyPart kp : primaryRowKeyParts) {
          LOG.info(kp);
        }
        dfaRowKeyParser = new DFARowKeyParser(primaryRowKeyParts, rkFieldInfoMap);
    }

    @Test
    public void testInitConstField() {
      dfaRowKeyParser.initConstField();
      Map<String, Pair<Integer, Integer>> constField = dfaRowKeyParser.getConstField();
      assertEquals(3, constField.size());
    }
  
  public void assertParseRowKey(String rowKey, int date, String event0, String event1, String event2, String event3, String event4, String event5, int uhash, int uid){
    BufferAllocator allocator = new DirectBufferAllocator();
    Map<String, ValueVector> vvMap = new HashMap<>();
    MaterializedField f = MaterializedField.create(new SchemaPath("uid", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.INT));
    ValueVector vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("uid", vv);
    f = MaterializedField.create(new SchemaPath("uhash", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.UINT1));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("uhash", vv);
    f = MaterializedField.create(new SchemaPath("date", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.INT));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("date", vv);

    f = MaterializedField.create(new SchemaPath("event0", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("event0", vv);
    f = MaterializedField.create(new SchemaPath("event1", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("event1", vv);
    f = MaterializedField.create(new SchemaPath("event2", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("event2", vv);
    f = MaterializedField.create(new SchemaPath("event3", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("event3", vv);
    f = MaterializedField.create(new SchemaPath("event4", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("event4", vv);
    f = MaterializedField.create(new SchemaPath("event5", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR));
    vv = TypeHelper.getNewVector(f, allocator);
    AllocationHelper.allocate(vv, BATCH_SIZE, 4);
    vvMap.put("event5", vv);

    Map<String, HBaseFieldInfo> projs = new HashMap<>();
    projs.put("uid", rkFieldInfoMap.get("uid"));
    projs.put("uhash", rkFieldInfoMap.get("uhash"));
    projs.put("date", rkFieldInfoMap.get("date"));
    projs.put("event0", rkFieldInfoMap.get("event0"));
    projs.put("event1", rkFieldInfoMap.get("event1"));
    projs.put("event2", rkFieldInfoMap.get("event2"));
    projs.put("event3", rkFieldInfoMap.get("event3"));
    projs.put("event4", rkFieldInfoMap.get("event4"));
    projs.put("event5", rkFieldInfoMap.get("event5"));
    
    byte[] rk = Bytes.toBytesBinary(rowKey);
    int index = 0;
    long cost = 0;
    dfaRowKeyParser.parseAndSet(rk, projs, vvMap, index, true);

    LOG.info(vvMap.get("uid").getAccessor().getObject(index));
    LOG.info(vvMap.get("uhash").getAccessor().getObject(index));
    LOG.info(vvMap.get("date").getAccessor().getObject(index));
    LOG.info(Bytes.toString((byte[])vvMap.get("event0").getAccessor().getObject(index)));
    LOG.info(Bytes.toString((byte[])vvMap.get("event1").getAccessor().getObject(index)));
    LOG.info(Bytes.toString((byte[])vvMap.get("event2").getAccessor().getObject(index)));
    LOG.info(Bytes.toString((byte[])vvMap.get("event3").getAccessor().getObject(index)));
    LOG.info(Bytes.toString((byte[])vvMap.get("event4").getAccessor().getObject(index)));
    LOG.info(Bytes.toString((byte[])vvMap.get("event5").getAccessor().getObject(index)));

    if(uid!=0){
      Assert.assertEquals(uid, vvMap.get("uid").getAccessor().getObject(index));
    }
    if(uhash!=0){
      byte[] tmp = new byte[4];
      tmp[3] = (Byte)vvMap.get("uhash").getAccessor().getObject(index);
      Assert.assertEquals(uhash, Bytes.toInt(tmp));
    }
    if(date!=0){
      Assert.assertEquals(date, vvMap.get("date").getAccessor().getObject(index));
    }
    if(event0!=null){
      Assert.assertEquals(event0, Bytes.toString((byte[])vvMap.get("event0").getAccessor().getObject(index)));
    }
    if(event1!=null){
      Assert.assertEquals(event1, Bytes.toString((byte[])vvMap.get("event1").getAccessor().getObject(index)));
    }
    if(event2!=null){
      Assert.assertEquals(event2, Bytes.toString((byte[])vvMap.get("event2").getAccessor().getObject(index)));
    }
    if(event3!=null){
      Assert.assertEquals(event3, Bytes.toString((byte[])vvMap.get("event3").getAccessor().getObject(index)));
    }
    if(event4!=null){
      Assert.assertEquals(event4, Bytes.toString((byte[])vvMap.get("event4").getAccessor().getObject(index)));
    }
    if(event5!=null){
      Assert.assertEquals(event5, Bytes.toString((byte[])vvMap.get("event5").getAccessor().getObject(index)));
    }
  }
  
  @Test
  public void testParseRowKey(){
    String rowKey1 = "20130918response.agei.report.241427s.pend.2s5s.\\xFF[\\x00\\x00\\x00@";
    assertParseRowKey(rowKey1, 20130918, "response", "agei", "report", "241427s", "pend", "2s5s", 50, 64);
    String rowKey2 = "20130710pay.sites.click.Jogos da Barbie.\\xFFj\\x00\\x00\\x00\\x90";
    assertParseRowKey(rowKey2, 20130710, "pay", "sites", "click", "Jogos da Barbie", null, null, 50, 144);
  }

    @Test
    public void testParseAndSetUidOnly() throws IOException {
      Map<String, ValueVector> vvMap = new HashMap<>();
      MaterializedField f = MaterializedField.create(new SchemaPath("uid", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.INT));
      ValueVector vv = TypeHelper.getNewVector(f, new DirectBufferAllocator());
      AllocationHelper.allocate(vv, BATCH_SIZE, 4);
      vvMap.put("uid", vv);

      Map<String, HBaseFieldInfo> projs = new HashMap<>();
      projs.put("uid", rkFieldInfoMap.get("uid"));

      String fileName = "/parser/rowkey.txt";
      File file = FileUtils.getResourceAsFile(fileName);
      int lineNum = 0;
      BufferedReader br = null;
      try {
        br = new BufferedReader(new FileReader(file));
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      String line = br.readLine();
      byte[] rk = Bytes.toBytesBinary(line);        
      int index = 0;
      long cost = 0;
      final int round = 10000000;
      for (int i = 0; i < 100; i++) {//warm up 
        dfaRowKeyParser.parseAndSet(rk, projs, vvMap, index, true);        
      }
      long t0 = System.nanoTime();
      for (int i = 0; i < round; i++) {
        dfaRowKeyParser.parseAndSet(rk, projs, vvMap, index, true);
        if (index == BATCH_SIZE - 1) {
          AllocationHelper.allocate(vv, BATCH_SIZE, 4);
          index = 0;
        }
        lineNum++;        
      }
     long t1 = System.nanoTime();
      vv.getMutator().setValueCount(10);
      LOG.info("Total speed: " + (int)(round/((t1-t0)/1e9)) + " line/sec");
      ValueVector.Accessor accessor = vv.getAccessor();
      Set<Integer> uidSet = new HashSet<>();
      for (int i=0; i<vv.getAccessor().getValueCount(); i++) {
        int uid = (Integer)accessor.getObject(i);
        uidSet.add(uid);
      }
      assertEquals(2, uidSet.size());
    }

  @Test
    public void testToInt() throws Exception{
      byte[] bytes = new byte[10];
      Arrays.fill(bytes, (byte)2);
      long sum = 0;
      final int round = 100000000;
      for (int i = 0; i < 10000; i++) {
        sum+=Bytes.toInt(bytes, 4);
        sum+=DFARowKeyParser.toInt(bytes, 4);
      }
      long t0 = System.nanoTime();
      for (int i = 0; i < round; i++) {
        sum += Bytes.toInt(bytes, i%5);
      }
      long t1 = System.nanoTime();
      for (int i = 0; i < round; i++) {
        sum += DFARowKeyParser.toInt(bytes, (i%5));
      }
      long t2 = System.nanoTime();
      System.out.println("sum:"+sum);
      System.out.println("time:"+(t1-t0)/1000+" vs "+(t2-t1)/1000);
    }



}
