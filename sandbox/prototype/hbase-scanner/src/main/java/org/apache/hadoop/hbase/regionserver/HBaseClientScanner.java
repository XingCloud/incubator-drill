package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-4
 * Time: 下午2:06
 * To change this template use File | Settings | File Templates.
 */
public class HBaseClientScanner implements XAScanner {
  private static Log LOG = LogFactory.getLog(HBaseClientScanner.class);

  private static final int cacheSize = 32 * 1024;
  private static final int batchSize = 32 * 1024;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  private Filter filter;
  private HTableInterface hTable;
  private ResultScanner scanner;

  public HBaseClientScanner(byte[] startRowKey, byte[] endRowKey, String tableName, Filter filter) {
    this.startRowKey = startRowKey;
    this.endRowKey = endRowKey;
    this.tableName = tableName;
    this.filter = filter;
    try {
      hTable = HBaseResourceManager.getInstance().getTable(tableName);
      Scan scan = initScan(startRowKey, endRowKey);
      scanner = hTable.getScanner(scan);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("Init hbase client scanner failure!", e);
      hTable = null;
    }

  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    Result[] hbresults = scanner.next(10000);
    if (hbresults == null || hbresults.length == 0) {
      return false;
    }
    for(Result result : hbresults){
      if (!result.isEmpty()) {
        for (KeyValue kv : result.raw()) {
          results.add(kv);
        }
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    if (hTable != null) {
      hTable.close();
    }
  }

  private Scan initScan(byte[] startRowKey, byte[] endRowKey) {
    Scan scan = new Scan(startRowKey, endRowKey);
//    scan.setCacheBlocks(false);
    scan.setBatch(batchSize);
    scan.setCaching(cacheSize);
    scan.setMaxVersions();
    if (filter != null) {
      scan.setFilter(filter);
    }
    return scan;
  }
}
