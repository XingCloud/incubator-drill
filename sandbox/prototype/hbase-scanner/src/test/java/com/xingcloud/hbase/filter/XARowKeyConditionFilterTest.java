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
public class XARowKeyConditionFilterTest {
    public static Logger logger= LoggerFactory.getLogger(XARowKeyConditionFilterTest.class);
    @Test
    public void testFilter() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        HTable table=new HTable(conf,"sof-dsk_deu");
        Scan scan=new Scan();
        scan.setStartRow(ByteUtils.toBytesBinary("20130101visit.a"));
        scan.setStopRow(ByteUtils.toBytesBinary("20130102visit.\\xFF"));
        List<RowKeyFilterCondition> filterConditionList=new ArrayList<>();
        filterConditionList.add(new RowKeyFilterPattern("20130101visit.auto"));
        filterConditionList.add(new RowKeyFilterRange("20130101visit.s","20130101visit.y") );
        filterConditionList.add(new RowKeyFilterPattern("20130102visit.auto"));
        filterConditionList.add(new RowKeyFilterRange("20130102visit.s","20130102visit.y"));
        XARowKeyPatternFilter filter=new XARowKeyPatternFilter(filterConditionList);
        scan.setFilter(filter);
        scan.setBatch(1000);
        scan.setCaching(16);
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
}
