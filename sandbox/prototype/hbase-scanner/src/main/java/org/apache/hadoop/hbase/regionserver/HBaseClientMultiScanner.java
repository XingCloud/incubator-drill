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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * User: liqiang
 * Date: 14-12-2
 * Time: 下午3:34
 * 使用SkipScanFilter需要每个rowkey都去分别比较一次，效率不高，直接换成scan查询
 */
public class HBaseClientMultiScanner implements XAScanner {
    private static Log LOG = LogFactory.getLog(HBaseClientMultiScanner.class);
    private static Pair<byte[], byte[]> uidRange = new Pair<>();
    static{
        byte[] first = new byte[]{0,0,0,0,0};
        byte[] second = new byte[]{(byte)255,(byte)255,(byte)255,(byte)255,(byte)255};
        uidRange.setFirst(first);
        uidRange.setSecond(second);
    }


    private static byte[] MAX = {-1};

    private static final int cacheSize = 16 * 1024;
    private static final int batchSize = 16 * 1024;
    private byte[] startRowKey;
    private byte[] endRowKey;
    private String tableName;
    private Filter filter;
//    private HTableInterface hTable;
    private List<KeyRange> slot;
    private List<Future<List<KeyValue>>> scans = new ArrayList<Future<List<KeyValue>>>();
    private int pos = 0;

    public HBaseClientMultiScanner(byte[] startRowKey, byte[] endRowKey, String tableName, Filter filter, List<KeyRange> slot) {
        LOG.info(" new HBaseClientMultiScanner ");
        System.out.println(" new HBaseClientMultiScanner ");
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
        this.tableName = tableName;
        this.filter = filter;
        this.slot = slot;
        try {
//            hTable = HBaseResourceManager.getInstance().getTable(tableName);
            init();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Init hbase client scanner failure!", e);
//            hTable = null;
        }

    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {

        if(pos < scans.size()){
            try {
                List<KeyValue> kvs = scans.get(pos).get();
                System.out.println("pos "+ pos +" get " + kvs.size() );
                for (KeyValue kv : kvs) {
                    results.add(kv);
                }
                pos ++;
                return true;
            } catch (Exception e) {
                throw new IOException("Error query hbase", e.getCause());
            }
        }
        return false;
    }

    private boolean init(){
        LOG.debug("HBaseClientMultiScanner init " + tableName + " " + (slot ==null ? 1 : slot.size() ));
        System.out.println("HBaseClientMultiScanner init " + tableName + " " + (slot ==null ? 1 : slot.size() ));

        if(slot == null || slot.size() == 0){
            scans.add(HBaseUtil.executor.submit(new InnerScanner(new KeyRange(startRowKey, true, endRowKey, false))));
        }else {
            for(KeyRange kr : slot){
                scans.add(HBaseUtil.executor.submit(new InnerScanner(kr)));
            }
        }
        return true;

    }

    @Override
    public void close() throws IOException {
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

        return new Pair<>(Bytes.add(startRow,MAX), Bytes.add(endRow, MAX));
    }

    private Scan initScan(byte[] startRowKey, byte[] endRowKey) {
//        Pair<byte[],byte[]> startStopRow = getStartStopRow(startRowKey,endRowKey);
//        System.out.println("startrow: " + Bytes.toStringBinary(startStopRow.getFirst()) + ", stoprow:" + Bytes.toStringBinary( startStopRow.getSecond()));
//        Scan scan = new Scan(startStopRow.getFirst(), startStopRow.getSecond());
        Scan scan = new Scan(startRowKey, endRowKey);
        scan.setCacheBlocks(false);
        scan.setBatch(batchSize);
        scan.setCaching(cacheSize);
        scan.setMaxVersions();
        if (filter != null) {
            scan.setFilter(filter);
        }
        return scan;
    }

    class InnerScanner implements Callable<List<KeyValue>>{

        KeyRange kr;
        public InnerScanner(KeyRange kr){
            this.kr = kr;
        }

        @Override
        public List<KeyValue> call() throws Exception {
            long begin = System.currentTimeMillis();
            ResultScanner iscanner = null;
            HTableInterface htable = null;
            try{
                htable = HBaseResourceManager.getInstance().getTable(tableName);
                Scan scan = initScan(kr.getLowerRange(), kr.getUpperRange());
                iscanner = htable.getScanner(scan);
                List<KeyValue> iresults = new ArrayList<KeyValue>();
                Result[] hbresults;
                while(true){ //一直往下查，直到有数据返回
                    hbresults = iscanner.next(10000);
                    if(hbresults == null || hbresults.length == 0){
                        break;
                    }
                    for (Result result : hbresults) {
                        if (!result.isEmpty()) {
                            for (KeyValue kv : result.raw()) {
                                iresults.add(kv);
                            }
                        }
                    }
                }
                LOG.info("InnerScanner scan " + tableName + " " + Bytes.toStringBinary(kr.getLowerRange()) + ", count "+ iresults.size() +", cost " + (System.currentTimeMillis() - begin) + "mills");
                return iresults;
            }finally {
                if(iscanner != null){
                    iscanner.close();
                }
                if(htable != null){
                    htable.close();
                }
            }
        }
    }
}
