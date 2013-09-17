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
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.parser.DFARowKeyParser;

import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
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
        dfaRowKeyParser = new DFARowKeyParser(primaryRowKeyParts, rkFieldInfoMap);
    }

    @Test
    public void testInitConstField() {
      dfaRowKeyParser.initConstField();
      Map<String, Pair<Integer, Integer>> constField = dfaRowKeyParser.getConstField();
      assertEquals(3, constField.size());
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
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      String line = null;
      int index = 0;
      long cost = 0;
      while ((line=br.readLine()) != null) {
        lineNum++;
        byte[] rk = Bytes.toBytesBinary(line);
        long st = System.nanoTime();
        dfaRowKeyParser.parseAndSet(rk, projs, vvMap, index++, true);
        cost += System.nanoTime() - st;
        if (index == BATCH_SIZE - 1) {
          AllocationHelper.allocate(vv, BATCH_SIZE, 4);
          index = 0;
        }
      }
      vv.getMutator().setValueCount(lineNum);
      LOG.info("Total cost: " + cost/1.0e9 + " sec");
      ValueVector.Accessor accessor = vv.getAccessor();
      Set<Integer> uidSet = new HashSet<>();
      for (int i=0; i<vv.getAccessor().getValueCount(); i++) {
        int uid = (Integer)accessor.getObject(i);
        uidSet.add(uid);
      }
      assertEquals(3, uidSet.size());
    }





}
