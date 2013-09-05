package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 8/22/13
 * Time: 12:53 PM
 */
public class XARegionScanner implements XAScanner{
  private static Log LOG = LogFactory.getLog(XARegionScanner.class);

  private MemstoresScanner memstoresScanner;
  private StoresScanner storesScanner;
  private KeyValue.KVComparator comparator;

  //Use to control version of one row
  private long versionCounter = 0;
  
  public XARegionScanner(HRegionInfo hRegionInfo, Scan scan) throws IOException {
    if(!scan.isFilesOnly()){
      memstoresScanner = new MemstoresScanner(hRegionInfo, scan);
    }
    
    if(!scan.isMemOnly()){
      storesScanner = new StoresScanner(hRegionInfo, scan);
    }
    comparator = hRegionInfo.getComparator();
    firstScan();
  }
  
  private KeyValue MSNext;
  private KeyValue SSNext;
  private KeyValue theNext;

  private void firstScan() throws IOException {
    MSNext = getKVFromMS();
    SSNext = getKVFromSS();
    theNext = getLowest(MSNext, SSNext);
  }

  public boolean next(List<KeyValue> results) throws IOException {
    if (theNext == null) {
      //Both memstore and hfile have no value at all
      return false;
    }
    KeyValue ret = null;
    while (results.size() < Helper.BATCH_SIZE + 1) {
      ret = theNext;
      if(theNext == MSNext){
        MSNext = getKVFromMS();
      }else{
        SSNext = getKVFromSS();
      }
      theNext = getLowest(MSNext, SSNext);
      if (theNext == null) {
        //The last one
        results.add(ret);
        return false;
      }
      //Remove duplicate kv
      if (!theNext.equals(ret)) {
        //Control version of each cell
        byte[] rowCurrent = ret.getRow();
        byte[] rowNext = theNext.getRow();
        if (Bytes.compareTo(rowCurrent, rowNext) == 0) {
          versionCounter++;
        } else {
          versionCounter = 0;
        }
        if (versionCounter < Helper.MAX_VERSIONS) {
          results.add(ret);
        }
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    LOG.info("XARegion scanner closed.");
    if(memstoresScanner != null){
      memstoresScanner.close();
    }  
    if(storesScanner != null){
      storesScanner.close();
    }
  }

  private Queue<KeyValue> MSKVCache = new LinkedList<>();
  
  public KeyValue getKVFromMS() throws IOException {
    if (null == memstoresScanner) return null;
    
    while (true){
      if (0 == MSKVCache.size()){
        List<KeyValue> results = new ArrayList<>();
        if(memstoresScanner.next(results)) {
          MSKVCache.addAll(results);
        } else {
          return null;
        }
      }

      KeyValue kv = MSKVCache.poll();

      if (Bytes.compareTo(kv.getRow(), Bytes.toBytes("flush")) == 0) {
        if (storesScanner != null){
          storesScanner.updateScanner(kv.getFamily(), theNext); //todo kv to seek
          if(SSNext == null){
            SSNext = getKVFromSS();
          }
        }
      } else {
        return kv;
      }
    }
  }

  private Queue<KeyValue> SSKVCache = new LinkedList<>();
  
  public KeyValue getKVFromSS() throws IOException {
    if(null == storesScanner) return null;
    if(0 == SSKVCache.size()){
      List<KeyValue> results = new ArrayList<>();
      boolean hasNext = storesScanner.next(results);
      SSKVCache.addAll(results);
    }
    return SSKVCache.poll();
  }

  private KeyValue getLowest(final KeyValue a, final KeyValue b) {
    if (null == a) {
      return b;
    }
    if (null == b) {
      return a;
    }
    return comparator.compareRows(a, b) <= 0 ? a: b;
  }  
}
