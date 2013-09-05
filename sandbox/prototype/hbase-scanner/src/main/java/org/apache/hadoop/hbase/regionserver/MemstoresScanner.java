package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.HBaseResourceManager;
import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-7
 * Time: 下午6:46
 * To change this template use File | Settings | File Templates.
 */
public class MemstoresScanner implements XAScanner {
  private static Logger LOG = LoggerFactory.getLogger(MemstoresScanner.class);

  private Scan scan;
  private ResultScanner rs;
  private AtomicLong numKV = new AtomicLong();
  private HRegionInfo hRegionInfo;

  public MemstoresScanner(HRegionInfo hRegionInfo, Scan scan) throws IOException {
    this.hRegionInfo = hRegionInfo;
    this.scan = scan;

    long st = System.nanoTime();

    // make sure that we only get the corresponding memstores for this region
    byte[] startRowKey = hRegionInfo.getStartKey();
    byte[] endRowKey = hRegionInfo.getEndKey();
    if (Bytes.compareTo(scan.getStartRow(), startRowKey) > 0 || Bytes.equals(startRowKey, HConstants.EMPTY_START_ROW)) {
      startRowKey = scan.getStartRow();
    }
    if (Bytes.compareTo(scan.getStopRow(), endRowKey) < 0 || Bytes.equals(endRowKey,HConstants.EMPTY_END_ROW))  {
      endRowKey = scan.getStopRow();
    }
    Scan memScan = new Scan(startRowKey, endRowKey);
    
    memScan.setMaxVersions();
    memScan.setMemOnly(true);
    if (scan.getFilter() != null)
      memScan.setFilter(scan.getFilter());
    memScan.addColumn(Bytes.toBytes("val"), Bytes.toBytes("val"));

    LOG.info("Init memstore scanner finished. Taken: " + (System.nanoTime() - st) / 1.0e9 + " sec");
    
    HTable table = (HTable) HBaseResourceManager.getInstance().getTable(hRegionInfo.getTableName()).getWrappedTable();
    try {
      rs = table.getScanner(memScan);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("Init memstore scanner failure! MSG: " + e.getMessage()); 
    }
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    Result r = rs.next();

    if (r == null) {
      return false;
    }
    KeyValue[] kvs = r.raw();
    if (kvs.length == 0) {
      return false;
    }
    for (KeyValue kv : kvs) {
      if (numKV.get()%100==0) {
        //Debug
        byte[] row = kv.getRow();
        long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(row);
        String event = HBaseEventUtils.getEventFromDEURowKey(row);
        String date = HBaseEventUtils.getDate(row);
        LOG.info("From Memstore: " + date + "\t" + event + "\t" + uid + "\t" + Bytes.toLong(kv.getValue()) + "\t" + kv.getTimestamp());
      }
      results.add(kv);
      numKV.incrementAndGet();
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (rs != null) {
      rs.close();
    }
    LOG.info("Memstore scanner closed. Total kv number form memstore: " + numKV);
  }

}
