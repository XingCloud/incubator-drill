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
      scanner.close();
    }
    LOG.info("HBase client scanner Average: " + totalCost/1.0e9/times + " sec");
  }

  public static void testDirectScanner(byte[] startRowKey, byte[] endRowKey, byte[] family, byte[] qualifier, String tableName, int times) throws IOException {
    LOG.info("Start to test direct scanner...");
    long totalCost = 0l;
    for (int i=0; i<times; i++) {
      long st = System.nanoTime();
      DirectScanner scanner = new DirectScanner(startRowKey, endRowKey, tableName, null, family, qualifier, false, false);
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
      scanner.close();
    }

    LOG.info("Direct client scanner Average: " + totalCost/1.0e9/times + " sec");
  }

  public static void testDiff(byte[] startRowKey, byte[] endRowKey, byte[] family, byte[] qualifier, String tableName) throws IOException {
    long st = System.nanoTime();
    Set<KeyValue> kvsFromClient = new HashSet<>();
    HBaseClientScanner scannerC = new HBaseClientScanner(startRowKey, endRowKey, tableName, null);
    List<KeyValue> results = new ArrayList<>();
    boolean done = false;
    long count = 0;
    long sum = 0;
    Set<Long> uids = new HashSet<>();
    do {
      results.clear();
      done = scannerC.next(results);
      for (KeyValue kv : results) {
        byte[] row = kv.getRow();
        long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(row);
        uids.add(uid);
        count++;
        sum += Bytes.toLong(kv.getValue());
        kvsFromClient.add(kv);
      }
    } while (done);
    long cost = (System.nanoTime()-st);
    LOG.info("Client scanner:\t" + count + " " + sum + " " + uids.size() + "   " + cost/1.0e9 + " sec");
    scannerC.close();

    Set<KeyValue> kvsFromDirect = new HashSet<>();
    st = System.nanoTime();
    DirectScanner scannerD = new DirectScanner(startRowKey, endRowKey, tableName, null, family, qualifier, false, false);
    results = new ArrayList<>();
    done = false;
    count = 0;
    sum = 0;
    uids = new HashSet<>();
    do {
      results.clear();
      done = scannerD.next(results);
      for (KeyValue kv : results) {
        byte[] row = kv.getRow();
        long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(row);
        uids.add(uid);
        count++;
        sum += Bytes.toLong(kv.getValue());
        kvsFromDirect.add(kv);
      }
    } while (done);
    LOG.info("Direct scanner:\t" + count + " " + sum + " " + uids.size() + "   " + cost/1.0e9 + " sec");
    scannerD.close();

    LOG.info("Start to compare...");
    LOG.info("Client kv size: " + kvsFromClient.size() + "\tDirect kv size: " + kvsFromDirect.size());
    for (KeyValue kv : kvsFromDirect) {
      if (!kvsFromClient.contains(kv)) {
        byte[] row = kv.getRow();
        long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(row);
        String event = HBaseEventUtils.getEventFromDEURowKey(row);
        String date = HBaseEventUtils.getDate(row);
        LOG.info(date + "\t" + event + "\t" + uid + "\t" + Bytes.toLong(kv.getValue()) + "\t" + kv.getTimestamp());
      }
    }

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
        testDirectScanner(srk, erk, null, null, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } else if (mode.equals("d")) {
      try {
        testDirectScanner(srk, erk, null, null, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } else if (mode.equals("c")) {
      try {
        testHBaseClientScanner(srk, erk, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } else if (mode.equals("test_family")) {
      byte[] family = Bytes.toBytes("val");
      byte[] qualifier = Bytes.toBytes("val");
      try {
        LOG.info("Direct scan with family...");
        testDirectScanner(srk, erk, family, qualifier, table, times);
        LOG.info("Direct scan without family...");
        testDirectScanner(srk, erk, null, null, table, times);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    } else if (mode.equals("diff")) {
      try {
        testDiff(srk, erk, null, null, table);
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    LOG.info("All tests finish.");
  }

}
