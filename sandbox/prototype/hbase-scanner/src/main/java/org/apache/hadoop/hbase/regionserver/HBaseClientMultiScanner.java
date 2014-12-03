package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.HBaseResourceManager;
import com.xingcloud.xa.hbase.model.KeyRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: liqiang
 * Date: 14-12-2
 * Time: 下午3:34
 * 使用SkipScanFilter需要每个rowkey都去分别比较一次，效率不高，直接换成scan查询
 */
public class HBaseClientMultiScanner implements XAScanner {
    private static Log LOG = LogFactory.getLog(HBaseClientMultiScanner.class);
    private static Pair<byte[], byte[]> uidRange = Helper.getLocalSEUidOfBucket(255, 255);
    static{
        uidRange.setFirst(Arrays.copyOfRange(uidRange.getFirst(), 3, uidRange.getFirst().length));
        uidRange.setSecond(Arrays.copyOfRange(uidRange.getSecond(), 3, uidRange.getSecond().length));
    }

    private static byte[] MAX = {-1};

    private static final int cacheSize = 32 * 1024;
    private static final int batchSize = 32 * 1024;
    private byte[] startRowKey;
    private byte[] endRowKey;
    private String tableName;
    private Filter filter;
    private HTableInterface hTable;
    private ResultScanner scanner;
    private List<KeyRange> slot;
    private int position = 0;

    public HBaseClientMultiScanner(byte[] startRowKey, byte[] endRowKey, String tableName, Filter filter, List<KeyRange> slot) {
        LOG.info(" new HBaseClientMultiScanner ");
        System.out.println(" new HBaseClientMultiScanner ");
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
        this.tableName = tableName;
        this.filter = filter;
        this.slot = slot;
        try {
            hTable = HBaseResourceManager.getInstance().getTable(tableName);
            nextScanner();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Init hbase client scanner failure!", e);
            hTable = null;
        }

    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        Result[] hbresults;

        while(true){ //一直往下查，直到有数据返回
            hbresults = scanner.next(10000);
            if ( (hbresults == null || hbresults.length == 0) && !nextScanner() ) {
                return false;
            }

            if(hbresults.length > 0){
                break;
            }
        }
        for (Result result : hbresults) {
            if (!result.isEmpty()) {
                for (KeyValue kv : result.raw()) {
                    results.add(kv);
                }
            }
        }
        return true;
    }

    private boolean nextScanner(){
        LOG.debug("nextScanner " + position + " " + (slot ==null ? 0: slot.size() ));
        System.out.println("HBaseClientMultiScanner nextScanner " + position + " " + (slot ==null ? 0: slot.size() ) );
        try{
            if(slot == null || slot.size() == 0){
                if(position == 0){
                    Scan scan = initScan(startRowKey, endRowKey);
                    position ++;
                    scanner = hTable.getScanner(scan);
                    return true;
                }
            }else if(position < slot.size()){
                KeyRange kr = slot.get(position);
                position ++;

                Scan scan = initScan(kr.getLowerRange(), kr.getUpperRange());
                if(scanner!=null){
                    scanner.close();
                }
                scanner = hTable.getScanner(scan);
                return true;
            }

        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Init hbase client scanner failure!", e);
            hTable = null;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        if(scanner != null){
            scanner.close();
        }
        if (hTable != null) {
            hTable.close();
        }
    }

    private Pair<byte[],byte[]> getStartStopRow(byte[] startRowKey, byte[] endRowKey){
        //开始和结束
        byte[] startRow = startRowKey;
        byte[] endRow = endRowKey;
        if(Bytes.equals(Bytes.tail(startRowKey,uidRange.getFirst().length), uidRange.getFirst()) &&
                Bytes.equals(Bytes.tail(endRowKey,uidRange.getSecond().length), uidRange.getSecond())){
                //TODO: 目前所有的start结束都为0000,end结束都为ffff,可以去掉这层判断
            byte[] start = Bytes.head(startRowKey, startRowKey.length - uidRange.getFirst().length -1);
            byte[] end = Bytes.head(endRowKey, endRowKey.length - uidRange.getSecond().length -1);
            System.out.println("start: " + Bytes.toStringBinary(start) + ", end:" + Bytes.toStringBinary(end));
            if(start.length > end.length){
                int len = start.length - end.length;
                byte[] tail = new byte[len];
                for(int i=0;i<len;i++){
                    tail[i] = (byte)255;
                }
                startRow = start;
                endRow = Bytes.add(end, tail);
            }else if(start.length < end.length){
                int len = end.length - start.length;
                byte[] tail = new byte[len];
                for(int i=0;i<len;i++){
                    tail[i] = (byte)0;
                }
                startRow = Bytes.add(start, tail);
                endRow = end;
            }else if(Bytes.equals(start,end)){
                startRow = start;
                endRow = end;
                endRow[endRow.length-1] = (byte)((int)endRow[endRow.length-1]+1);
            }else{
                startRow = start;
                endRow = end;
            }

        }
        System.out.println("startrow: " + Bytes.toStringBinary(startRow) + ", stoprow:" + Bytes.toStringBinary(endRow));
        return new Pair<>(Bytes.add(startRow,MAX), Bytes.add(endRow, MAX));
    }

    private Scan initScan(byte[] startRowKey, byte[] endRowKey) {
        Pair<byte[],byte[]> startStopRow = getStartStopRow(startRowKey,endRowKey);
        Scan scan = new Scan(startStopRow.getFirst(), startStopRow.getSecond());
        scan.setBatch(batchSize);
        scan.setCaching(cacheSize);
        scan.setMaxVersions();
        if (filter != null) {
            scan.setFilter(filter);
        }
        return scan;
    }
}
