package com.xingcloud.hbase.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import javax.xml.crypto.dsig.keyinfo.KeyValue;


/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/5/13
 * Time: 2:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestHBaseResourceManager {
    @Test
    public void testHTable() throws IOException {
        Configuration conf= HBaseConfiguration.create();
        HTable table=HBaseResourceManager.getInstance().getTable("deu_sof-dsk");
        //HTable table=HBaseResourceManager.getInstance().getTable("property_testtable_100W_index");
        FileSystem fs=FileSystem.get(conf);
        System.out.println(fs.getHomeDirectory());
        System.out.println(fs.getStatus());
        System.out.println(table.getTableDescriptor());
        System.out.println(table.getConfiguration());

        //HTable table=new HTable(conf,"sof-dsk_deu");
        Scan scan=new Scan();

        byte[] srk= Bytes.toBytes("20121228");
        byte[] enk= Bytes.toBytes("20121230");
        /*
        byte[] srk;
        byte[] enk;
        byte[] propId=Bytes.toBytes((short)9);
        byte[] srtDay=Bytes.toBytes("20121201");
        byte[] endDay=Bytes.toBytes("20121202");
        srk=HBaseUserUtils.getRowKey(propId, srtDay);
        enk=HBaseUserUtils.getRowKey(propId,endDay);
        */
        scan.setStartRow(srk);
        scan.setStopRow(enk);
        //Filter filter=new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
        //        new BinaryComparator(Bytes.toBytes("20121127visit")));
        Filter filter=new SingleColumnValueFilter(
                Bytes.toBytes("val"),
                Bytes.toBytes("val"),
                CompareFilter.CompareOp.LESS,
                new BinaryComparator(Bytes.toBytes(1000l)));
        //scan.setFilter(filter);
        List<Filter> filters=new ArrayList<>();
        filters.add(filter);
        FilterList filterList=new FilterList(filters);
        //scan.setFilter(filterList);
        scan.setCaching(1000);
        scan.setBatch(8);
        long ts1=System.currentTimeMillis();
        ResultScanner result=table.getScanner(scan);
        int recordCount=0;
        for(Result res: result){
            //System.out.println(res);
            KeyValue[] kvs=res.raw();
            //recordCount++;
            //System.out.println(kvs.length);

            for( KeyValue kv: res.raw()){
                System.out.println(kv);
                recordCount++;
            }

        }
        long ts2=System.currentTimeMillis();
        System.out.println(ts2-ts1+" ms used. recordCount "+recordCount);
        byte[][] srks=table.getStartKeys();
        for(int i=0;i<srks.length;i++)
            System.out.println(srks[i]);
    }
}
