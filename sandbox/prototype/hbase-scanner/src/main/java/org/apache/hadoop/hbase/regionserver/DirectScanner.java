package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.*;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.util.HBaseEventUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-7
 * Time: 下午5:07
 * To change this template use File | Settings | File Templates.
 */
public class DirectScanner implements XAScanner {
  private static Logger LOG = LoggerFactory.getLogger(DirectScanner.class);

  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  private Filter filter;

  private List<XAScanner> scanners;

  boolean isMemOnly = false;
  boolean isFileOnly = false;
  private int currentIndex = 0;
  private List<HRegionInfo> regionList;
  private XAScanner currentScanner;
  private boolean hasNext = true;
  private Scan scan;

  private AtomicLong numKV = new AtomicLong();

  public DirectScanner(byte[] startRowKey, byte[] endRowKey, String tableName,
                       boolean isFileOnly, boolean isMemOnly) throws IOException {
    this(startRowKey, endRowKey, tableName, null, null, null, isFileOnly, isMemOnly);
  }

  public DirectScanner(byte[] startRowKey, byte[] endRowKey, String tableName, Filter filter,
                       boolean isFileOnly, boolean isMemOnly) {
    this(startRowKey, endRowKey, tableName, filter, null, null, isFileOnly, isMemOnly);
  }

  public DirectScanner(byte[] startRowKey, byte[] endRowKey, String tableName, Filter filter,
                       byte[] family, byte[] qualifier,
                       boolean isFileOnly, boolean isMemOnly) {
    this.isFileOnly = isFileOnly;
    this.isMemOnly = isMemOnly;
    this.startRowKey = startRowKey;
    this.endRowKey = endRowKey;
    this.tableName = tableName;
    this.filter = filter;

    //set scan
    this.scan = new Scan(startRowKey, endRowKey);
    scan.setMaxVersions();
    scan.setBatch(Helper.BATCH_SIZE);
    scan.setCaching(Helper.CACHE_SIZE);
    scan.setMemOnly(isMemOnly);
    scan.setFilesOnly(isFileOnly);
    if (filter != null)
      scan.setFilter(filter);
    if (family != null && qualifier != null) {
      scan.addColumn(family, qualifier);
    } else {
      scan.addColumn(Helper.DEFAULT_FAM, Helper.DEFAULT_COL);
    }

    // get regions 
    Pair<byte[], byte[]> seKey = new Pair(startRowKey, endRowKey);
    HTable table = null;
    try {
      table = (HTable) HBaseResourceManager.getInstance().getTable(Bytes.toBytes(tableName)).getWrappedTable();
      this.regionList = Helper.getRegionInfoList(table, seKey);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("Init Direct scanner failure! MSG: " + e.getMessage());
    }

    LOG.info("Number of regions: " + regionList.size() + " for " + tableName + " " + startRowKey + " " + endRowKey);
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    if (!hasNext || regionList.size() == 0) {
      return false;
    }

    if(currentScanner == null){
      currentScanner = new XARegionScanner(regionList.get(currentIndex), scan);
    }
    hasNext = currentScanner.next(results);
    numKV.addAndGet(results.size());
    if (!hasNext) {
      //Move to next region
      currentScanner.close();
      currentIndex++;
      if (currentIndex == regionList.size()){
        currentScanner = null;
        return false;
      }
      currentScanner = new XARegionScanner(regionList.get(currentIndex), scan);
      hasNext = true;
    }

    return hasNext;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Direct scanner closed. Total records from memstore and hfile: " + numKV.get());
  }

  public static void main(String[] args) throws IOException {
    String tableName = args[0];
    byte[] srkPre = Bytes.toBytes(args[1]);
    byte[] erkPre = Bytes.toBytes(args[2]);
    int buckets = Integer.parseInt(args[3]);
    int len = Integer.parseInt(args[4]);
    File file = new File("/tmp/ds.txt");
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));

    Pair<byte[], byte[]> uidRange = Helper.getLocalSEUidOfBucket(buckets, len);
    uidRange.setFirst(Arrays.copyOfRange(uidRange.getFirst(), 3, uidRange.getFirst().length));
    uidRange.setSecond(Arrays.copyOfRange(uidRange.getSecond(),
            3, uidRange.getSecond().length));
    byte[] MAX = {-1};

    byte[] srk = Helper.bytesCombine(srkPre, MAX, uidRange.getFirst());
    byte[] erk = Helper.bytesCombine(erkPre, MAX, uidRange.getSecond());

    System.out.println("Start row: " + Bytes.toStringBinary(srk) + "\tEnd row: " + Bytes.toStringBinary(erk));
    boolean isFileOnly = false;
    boolean isMemOnly = false;

    buckets += len;
    List<KeyRange> slot = new ArrayList<>();
    KeyRange range = new KeyRange(srk, true, erk, false);
    slot.add(range);
    Filter filter = new SkipScanFilter(slot, uidRange);

    DirectScanner scanner = new DirectScanner(srk, erk, tableName, filter, isFileOnly, isMemOnly);
    long counter = 0;
    long sum = 0;
    long st = System.nanoTime();
    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean done = false;
    Set<Integer> uids = new HashSet<>();
    try {
      do {
        results.clear();
        done = scanner.next(results);
        for (KeyValue kv : results) {
          int uid = Helper.getUidOfIntFromDEURowKey(kv.getRow());
          uids.add(uid);
          counter++;
          long sumTmp = Bytes.toLong(kv.getValue());
          sum += sumTmp;
          String event = Helper.getEvent(kv.getRow());
          int bucket = Helper.getBucketNum(kv.getRow());
          writer.write(event + "\t" + bucket + "\t" + uid + "\t"
                  + sumTmp + "\t" + kv.getTimestamp() + "\n");
        }

      } while (done);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
    writer.flush();
    writer.close();
    LOG.info("Scan finish. Total rows: " + counter + " Taken: " + (System.nanoTime() - st) / 1.0e9 + " sec");
    LOG.info("Uids number: " + uids.size() + "\tCount: " + counter + "\tSum: " + sum);
  }


}