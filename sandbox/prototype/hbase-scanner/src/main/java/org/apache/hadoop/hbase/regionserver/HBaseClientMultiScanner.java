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

import java.io.IOException;
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
        this.startRowKey = startRowKey;
        this.endRowKey = endRowKey;
        this.tableName = tableName;
        this.filter = filter;
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
        try{
            if(slot == null || slot.size() == 0){
                if(position == 0){
                    Scan scan = initScan(startRowKey, endRowKey);
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

    private Scan initScan(byte[] startRowKey, byte[] endRowKey) {
        Scan scan = new Scan(startRowKey, endRowKey);
        scan.setBatch(batchSize);
        scan.setCaching(cacheSize);
        scan.setMaxVersions();
        if (filter != null) {
            scan.setFilter(filter);
        }
        return scan;
    }
}
