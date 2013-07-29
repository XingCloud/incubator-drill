package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.HBaseResourceManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/2/13
 * Time: 8:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableScanner implements XAScanner {
    private static Logger LOG = LoggerFactory.getLogger(TableScanner.class);

    private byte[] startRowKey;
    private byte[] endRowKey;
    private String tableName;
    private Filter filter;
    private List<Filter> filterList;

    private List<DataScanner> scanners;

    boolean isMemOnly = false;
    boolean isFileOnly = false;
    private int currentIndex = 0;

    private long startTimeStamp=Long.MIN_VALUE;
    private long stopTimeStamp=Long.MAX_VALUE;

    private int scanCache=1024;
    private int scanBatch=8;



    public TableScanner(byte[] startRowKey, byte[] endRowKey, String tableName, boolean isFileOnly, boolean isMemOnly) {
        this(startRowKey, endRowKey, tableName, null, isFileOnly, isMemOnly);
    }

    public TableScanner(byte[] startRowKey,byte[] endRowKey,String tableName,List<Filter> filterList,
                        boolean isFileOnly,boolean isMemOnly){
        this(startRowKey, endRowKey, tableName, filterList, isFileOnly, isMemOnly,Long.MIN_VALUE,Long.MAX_VALUE);
    }
    public TableScanner(byte[] startRowKey,byte[] endRowKey,String tableName,List<Filter> filterList,
                        boolean isFileOnly,boolean isMemOnly,long startVersion,long stopVersion){
        this.isFileOnly = isFileOnly;
        this.isMemOnly = isMemOnly;
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
        this.tableName = tableName;
        if(filterList!=null&&filterList.size()!=0)
            this.filterList = filterList;
        else this.filterList=null;
        if(startVersion!=Long.MIN_VALUE)this.startTimeStamp=startVersion;
        if(stopVersion!=Long.MAX_VALUE)this.stopTimeStamp=stopVersion;
        try {
            initScanner();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Table scanner init failure! MSG: " + e.getMessage());
        }
    }


    private void initScanner() throws IOException {
        List<Pair<byte[], byte[]>> seKeyList = new ArrayList<Pair<byte[], byte[]>>();
        Pair<byte[], byte[]> pair = new Pair(startRowKey, endRowKey);
        seKeyList.add(pair);
        HTable table = HBaseResourceManager.getInstance().getTable(Bytes.toBytes(tableName));

        scanners = new ArrayList<DataScanner>();
        long st = System.nanoTime();

        if (!isFileOnly) {
            LOG.info("Begin to init memstore scanner...");
            st = System.nanoTime();
            Scan memScan = new Scan(startRowKey, endRowKey);
            //memScan.setMaxVersions();
            memScan.setCaching(scanCache);
            memScan.setBatch(scanBatch);
            if(!(startTimeStamp==Long.MIN_VALUE&&stopTimeStamp==Long.MAX_VALUE))
                memScan.setTimeRange(startTimeStamp,stopTimeStamp);
            //memScan.setMemOnly(true);
            //memScan.addColumn(Bytes.toBytes("val"), Bytes.toBytes("val"));
            //if (filter != null)
            //    memScan.setFilter(filter);
            if(filterList!=null)
            {
                FilterList filterList1=new FilterList(filterList);
                memScan.setFilter(filterList1);
            }
            MemstoreScanner memstoreScanner = new MemstoreScanner(table, memScan);
            scanners.add(memstoreScanner);
            LOG.info("Init memstore scanner finished. Taken: " + (System.nanoTime() - st) / 1.0e9 + " sec");
        }
        /*
        if (!isMemOnly) {
            LOG.info("Begin to init store file scanner...");
            List<HRegionInfo> regionInfoList = getRegionInfoList(table, seKeyList);
            LOG.info("Number of regions: " + regionInfoList.size() + " for " + tableName + " " + startRowKey + " " + endRowKey);

            for (HRegionInfo regionInfo : regionInfoList) {
                LOG.info("Init hfile scanner of " + regionInfo.toString());
                Scan fileScan = new Scan(startRowKey, endRowKey);
                if (filter != null)
                    fileScan.setFilter(filter);
                fileScan.addColumn(Bytes.toBytes("val"), Bytes.toBytes("val"));

                HFileScanner regionScanner = new HFileScanner(regionInfo, fileScan);
                scanners.add(regionScanner);
            }
            LOG.info("Init hfile scanner finished. Taken: " + (System.nanoTime() - st) / 1.0e9 + " sec");
        }
        */


    }

    public static List<HRegionInfo> getRegionInfoList(HTable hTable, List<Pair<byte[], byte[]>> seKeyList) throws IOException {
        Set<HRegionInfo> hRegionInfoSet = new HashSet<HRegionInfo>();
        NavigableMap<HRegionInfo, ServerName> regionInfoMap = hTable.getRegionLocations();
        if (regionInfoMap.size() == 1) {
            /*There is only one region*/
            return new ArrayList<HRegionInfo>(regionInfoMap.keySet());
        }
        for (Pair<byte[], byte[]> seKey : seKeyList) {
            List<HRegionInfo> regionInfoListTmp = getRegionInfoList(hTable, seKey.getFirst(), seKey.getSecond());
            for (HRegionInfo hRegionInfo : regionInfoListTmp) {
                hRegionInfoSet.add(hRegionInfo);
            }
        }
        List<HRegionInfo> regionInfoList = new ArrayList<HRegionInfo>(hRegionInfoSet);
        if (regionInfoList.size() == 0) {
            throw new IOException("Can't get region info of " + hTable.getTableName());
        }

        StringBuilder summary = new StringBuilder("Calling region list:\n");
        for (HRegionInfo ri : regionInfoList) {
            summary.append(ri.getRegionNameAsString()).append("\n");
        }
        LOG.info(summary.toString());
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

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        if (scanners.size() == 0)
            return false;
        DataScanner regionScanner = scanners.get(currentIndex);
        boolean next = regionScanner.next(results);
        if (!next && currentIndex < scanners.size() - 1) {
            /* Move to next scanner */
            currentIndex++;
            return true;
        } else {
            /* All finish */
            return next;
        }
    }

    @Override
    public void close() throws IOException {
        for (XAScanner regionScanner : scanners) {
            if (regionScanner != null) {
                regionScanner.close();
            }
        }
    }

    public static void main(String[] args) {
        String tableName = args[0];
        String srk = args[1];
        String erk = args[2];
        boolean isMemOnly = Boolean.parseBoolean(args[3]);
        boolean isFileOnly = Boolean.parseBoolean(args[4]);
        TableScanner scanner = new TableScanner(Bytes.toBytes(srk), Bytes.toBytes(erk), tableName, isMemOnly, isFileOnly);
        long counter = 0;
        long st = System.nanoTime();
        List<KeyValue> results = new ArrayList<KeyValue>();
        boolean done = false;
        try {
            do {
                results.clear();
                done = scanner.next(results);
                for (KeyValue kv : results) {
                    if (counter % 1000 == 0 || !done) {
                        LOG.info(Bytes.toString(kv.getRow()));
                    }
                    counter++;
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
        LOG.info("Scan finish. Total rows: " + counter + " Taken: " + (System.nanoTime() - st) / 1.0e9 + " sec");
    }
}