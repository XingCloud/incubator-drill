package com.xingcloud.hbase.filter;

import com.xingcloud.xa.hbase.filter.XARowKeyPatternFilter;
import com.xingcloud.xa.hbase.util.ByteUtils;
import com.xingcloud.xa.hbase.util.rowkeyCondition.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/4/13
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestXARowKeyConditionFilter {
    public static Logger logger= LoggerFactory.getLogger(TestXARowKeyConditionFilter.class);
    /*
    @Test
    public void testFilter() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        byte[] srk=Bytes.toBytesBinary("20130101");
        byte[] enk=Bytes.toBytesBinary("20130103visit.\\xFF");
        HTable table=new HTable(conf,"sof-dsk_deu");
        Scan scan=new Scan();
        scan.setStartRow(srk);
        scan.setStopRow(enk);
        List<RowKeyFilterCondition> filterConditionList=new ArrayList<>();
        String uidSrt= Bytes.toStringBinary(Bytes.tail(Bytes.toBytes(0l),5));
        String uidEnd= Bytes.toStringBinary(new byte[]{-1,-1,-1,-1,-1});
        //filterConditionList.add(new RowKeyFilterPattern("20130101visit.auto.\\xFF",uidSrt,uidEnd));
        filterConditionList.add(new RowKeyFilterRange("20130101z","20130104d",uidSrt,uidEnd));
        //filterConditionList.add(new RowKeyFilterRange("20130102z","20130103b"));
        //filterConditionList.add(new RowKeyFilterRange("20130102v","20130102y"));
        //filterConditionList.add(new RowKeyFilterRange("20130101visit.s","20130101visit.y") );
        //filterConditionList.add(new RowKeyFilterRange("20130102","20130102visit.y"));
        XARowKeyPatternFilter filter=new XARowKeyPatternFilter(filterConditionList);
        scan.setFilter(filter);
        scan.setBatch(1000);
        scan.setCaching(32);
        ResultScanner resultScanner=table.getScanner(scan);
        logger.info("hhhh");
        long t1=System.currentTimeMillis();
        int count=0;
        for(Result res: resultScanner){
             for(KeyValue kv :res.raw()){
                 //logger.info(kv.toString());
                 count++;
             }
        }
        long t2=System.currentTimeMillis();
        logger.info("use time "+(t2-t1)+" count "+count);
    }
    @Test
    public void testFilterOnline() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        byte[] srk=Bytes.toBytesBinary("20130101");
        byte[] enk=Bytes.toBytesBinary("20130104visit.\\xFF");
        String tableName=System.getenv("table");
        if(tableName==null)
            tableName="sof-dsk_deu";
        List<RowKeyFilterCondition> filterConditionList=new ArrayList<>();
        String bucket=System.getenv("bucketNum");
        int bucketNum;
        if(bucket==null)
            bucketNum=127;
        else
            bucketNum=Integer.parseInt(bucket);
        if(bucketNum>127)bucketNum=bucketNum-256;
        String uidSrt= Bytes.toStringBinary(Bytes.tail(Bytes.toBytes(0l),5));
        String uidEnd= Bytes.toStringBinary(new byte[]{(byte)bucketNum,-1,-1,-1,-1});
        //filterConditionList.add(new RowKeyFilterPattern("20130101visit.auto.\\xFF",uidSrt,uidEnd));
        filterConditionList.add(new RowKeyFilterRange("20130101z","20130104d",uidSrt,uidEnd));
        //filterConditionList.add(new RowKeyFilterRange("20130102z","20130103b"));
        //filterConditionList.add(new RowKeyFilterRange("20130102v","20130102y"));
        //filterConditionList.add(new RowKeyFilterRange("20130101visit.s","20130101visit.y") );
        //filterConditionList.add(new RowKeyFilterRange("20130102","20130102visit.y"));
        XARowKeyPatternFilter filter=new XARowKeyPatternFilter(filterConditionList);
        DirectScanner scanner=new DirectScanner(srk,enk,tableName,filter,false,false);
        List<KeyValue> kvs=new ArrayList<KeyValue>();
        logger.info("hhhh");
        int count=0;
        long t1=System.currentTimeMillis();
        while(scanner.next(kvs)){
            count+=kvs.size();
        }
        long t2=System.currentTimeMillis();
        logger.info("use time "+(t2-t1)+" count "+count);
    }
    */
}
