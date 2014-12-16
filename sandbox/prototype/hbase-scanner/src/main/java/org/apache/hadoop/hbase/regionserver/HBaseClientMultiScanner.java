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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
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


    private static final int cacheSize = 16 * 1024;
    private static final int batchSize = 16 * 1024;
    private byte[] startRowKey;
    private byte[] endRowKey;
    private String tableName;
    private Filter filter;
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
            init();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Init hbase client scanner failure!", e);
        }

    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {

        while(true){
            if(pos < scans.size()){
                try {
                    List<KeyValue> kvs = scans.get(pos).get();
                    pos ++;
                    if(kvs.size() > 0){
                        for (KeyValue kv : kvs) {
                            results.add(kv);
                        }
                        return true;
                    }
                } catch (Exception e) {
                    throw new IOException("Error query hbase", e.getCause());
                }
            }else{
                break;
            }
        }

        return false;
    }

    private boolean init(){
        LOG.debug("HBaseClientMultiScanner init " + tableName + " " + (slot ==null ? 1 : slot.size() ));

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

    private Scan initScan(byte[] startRowKey, byte[] endRowKey) {
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
                    hbresults = iscanner.next(20000);
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
