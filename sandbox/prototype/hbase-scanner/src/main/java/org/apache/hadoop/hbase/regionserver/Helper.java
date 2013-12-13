package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 8/22/13
 * Time: 9:45 AM
 */
public class Helper {

  private static Logger LOG = LoggerFactory.getLogger(Helper.class);

  public static final byte[] DEFAULT_FAM = Bytes.toBytes("val");
  public static final byte[] DEFAULT_COL = Bytes.toBytes("val");
  public static final int BATCH_SIZE = 63 * 1024;
  public static final int CACHE_SIZE = 16 * 1024;
  public static final int MAX_VERSIONS = 2000;

  public static List<HRegionInfo> getRegionInfoList(HTable hTable, Pair<byte[], byte[]> seKey) throws IOException {
    long st = System.nanoTime();
    Set<HRegionInfo> hRegionInfoSet = new HashSet<>();
    NavigableMap<HRegionInfo, ServerName> regionInfoMap = hTable.getRegionLocations();
    LOG.info("Get region location taken: " + (System.nanoTime()-st)/1.0e9 + " sec");

    if (regionInfoMap.size() == 1) {
      /*There is only one region*/
      return new ArrayList<HRegionInfo>(regionInfoMap.keySet());
    }
    List<HRegionInfo> regionInfoList = getRegionInfoList(hTable, seKey.getFirst(), seKey.getSecond());
    if (regionInfoList.size() == 0) {
      throw new IOException("Can't get region info of " + hTable.getTableName());
    }

    StringBuilder summary = new StringBuilder("Calling region list:\n");
    for (HRegionInfo ri : regionInfoList) {
      summary.append(ri.getRegionNameAsString()).append("\n");
    }
    LOG.info(summary.toString() + "Total Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
    return regionInfoList;
  }

  public static List<HRegionInfo> getRegionInfoList(HTable hTable, byte[] startRowKey, byte[] endRowKey) throws IOException {
    NavigableMap<HRegionInfo, ServerName> regionInfoMap = hTable.getRegionLocations();

    Map<ServerName, List<HRegionInfo>> counterMap = new HashMap<ServerName, List<HRegionInfo>>();

    Set<HRegionInfo> regionInfoSet = regionInfoMap.keySet();
    List<HRegionInfo> regionInfoList = new ArrayList<HRegionInfo>();

    for (HRegionInfo hRegionInfo : regionInfoSet) {
      byte[] startKey = hRegionInfo.getStartKey();
      byte[] endKey = hRegionInfo.getEndKey();
      if (Bytes.compareTo(startRowKey, startKey) >= 0) {
        if (Bytes.compareTo(startRowKey, endKey) < 0 || Bytes.equals(endKey, HConstants.EMPTY_END_ROW)) {
          regionInfoList.add(hRegionInfo);
          ServerName sn = regionInfoMap.get(hRegionInfo);
          if (counterMap.containsKey(sn)) {
            List<HRegionInfo> list = counterMap.get(sn);
            list.add(hRegionInfo);
            counterMap.put(sn, list);
          } else {
            List<HRegionInfo> list = new ArrayList<HRegionInfo>();
            list.add(hRegionInfo);
            counterMap.put(sn, list);
          }
        }
      } else if (Bytes.equals(endRowKey, HConstants.EMPTY_END_ROW) || Bytes.compareTo(startKey, endRowKey) <= 0) {
        regionInfoList.add(hRegionInfo);
        ServerName sn = regionInfoMap.get(hRegionInfo);
        if (counterMap.containsKey(sn)) {
          List<HRegionInfo> list = counterMap.get(sn);
          list.add(hRegionInfo);
          counterMap.put(sn, list);
        } else {
          List<HRegionInfo> list = new ArrayList<HRegionInfo>();
          list.add(hRegionInfo);
          counterMap.put(sn, list);
        }
      }

    }

    return regionInfoList;
  }

  public static byte[] bytesCombine(byte[]... bytesArrays){
    int length = 0;
    for (byte[] bytes: bytesArrays){
      length += bytes.length;
    }
    byte[] combinedBytes = new byte[length];
    int index = 0;
    for (byte[] bytes: bytesArrays){
      for(byte b: bytes){
        combinedBytes[index] = b;
        index++;
      }
    }
    return combinedBytes;
  }

  public static byte[] produceTail(boolean begin) {
    byte[] tail = new byte[6];
    tail[0] = (byte)255;
    if (begin) {
      for (int i=1; i<6; i++) {
        tail[i] = 0;
      }
    } else {
      for (int i=1; i<6; i++) {
        tail[i] = (byte)255;
      }
    }
    return tail;
  }

  public static Pair<byte[], byte[]> getLocalSEUidOfBucket(int startBucketPos, int offsetBucketLen) {
    long endBucket = 0;
    if (startBucketPos + offsetBucketLen >= 255) {
      endBucket = (1l << 40) - 1l;
    } else {
      endBucket = startBucketPos + offsetBucketLen;
      endBucket = endBucket << 32;
    }
    long startBucket = (long)startBucketPos << 32;
    return new Pair(Bytes.toBytes(startBucket), Bytes.toBytes(endBucket));
  }

  public static int getUidOfIntFromDEURowKey(byte[] rk) {
    byte[] uid = Arrays.copyOfRange(rk, rk.length-4, rk.length);
    return Bytes.toInt(uid);
  }

  public static int getBucketNum(byte[] rk) {
    byte[] prefix = {0,0,0};
    byte[] bucket = Arrays.copyOfRange(rk, rk.length-5, rk.length-4);
    return Bytes.toInt(bytesCombine(prefix, bucket));
  }

  public static String getEvent(byte[] rk) {
    return Bytes.toString(Arrays.copyOfRange(rk, 8, rk.length-6));
  }
}
