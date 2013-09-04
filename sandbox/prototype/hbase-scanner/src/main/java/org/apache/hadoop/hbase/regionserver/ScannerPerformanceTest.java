package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-4
 * Time: 下午2:49
 * To change this template use File | Settings | File Templates.
 */
public class ScannerPerformanceTest {
  private static Log LOG = LogFactory.getLog(ScannerPerformanceTest.class);

  public static void testHBaseClientScanner(byte[] startRowKey, byte[] endRowKey, String tableName, int times) throws IOException {
    LOG.info("Start to test hbase client performance...");
    long totalCost = 0l;
    for (int i=0; i<times; i++) {
      long st = System.nanoTime();
      HBaseClientScanner scanner = new HBaseClientScanner(startRowKey, endRowKey, tableName, null);
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean done = false;
      long count = 0;
      long sum = 0;
      Set<Long> uids = new HashSet<>();
      do {
        results.clear();
        done = scanner.next(results);
        for (KeyValue kv : results) {
          byte[] row = kv.getRow();
          long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(row);
          uids.add(uid);
          count++;
          sum += Bytes.toLong(kv.getValue());
        }
      } while (done);
      long cost = (System.nanoTime()-st);
      totalCost += cost;
      LOG.info(i + ":\t" + count + " " + sum + " " + uids.size() + "   " + cost/1.0e9 + " sec");
    }
    LOG.info("HBase client scanner Average: " + totalCost/1.0e9/times + " sec");
  }

  public static void testDirectScanner(byte[] startRowKey, byte[] endRowKey,  String tableName, int times) throws IOException {
    LOG.info("Start to test direct scanner...");
    long totalCost = 0l;
    for (int i=0; i<times; i++) {
      long st = System.nanoTime();
      DirectScanner scanner = new DirectScanner(startRowKey, endRowKey, tableName, null, false, false);
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean done = false;
      long count = 0;
      long sum = 0;
      Set<Long> uids = new HashSet<>();
      do {
        results.clear();
        done = scanner.next(results);
        for (KeyValue kv : results) {
          byte[] row = kv.getRow();
          long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(row);
          uids.add(uid);
          count++;
          sum += Bytes.toLong(kv.getValue());
        }
      } while (done);
      long cost = (System.nanoTime()-st);
      totalCost += cost;
      LOG.info(i + ":\t" + count + " " + sum + " " + uids.size() + "   " + cost/1.0e9 + " sec");
    }
    LOG.info("Direct client scanner Average: " + totalCost/1.0e9/times + " sec");
  }

  public static void main(String[] args) {
    String table = args[0];
    byte[] srk = Bytes.toBytes(args[1]);
    byte[] erk = Bytes.toBytes(args[2]);
    int times = Integer.parseInt(args[3]);
    String mode = args[4];
    if (mode.equals("compare")) {
      try {
        testHBaseClientScanner(srk, erk, table, times);
        testDirectScanner(srk, erk, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } else if (mode.equals("d")) {
      try {
        testDirectScanner(srk, erk, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } else if (mode.equals("c")) {
      try {
        testHBaseClientScanner(srk, erk, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    LOG.info("All tests finish.");
  }

}
