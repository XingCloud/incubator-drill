package com.xingcloud.hbase.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

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
        HTable table=HBaseResourceManager.getInstance().getTable("sof-dsk_deu");
        FileSystem fs=FileSystem.get(conf);
        System.out.println(fs.getHomeDirectory());
        System.out.println(fs.getStatus());
        System.out.println(table.getTableDescriptor());
        System.out.println(table.getConfiguration());

        //HTable table=new HTable(conf,"sof-dsk_deu");
        Scan scan=new Scan();
        byte[] srk= Bytes.toBytes("20121106heartbeat.xFFxC7x00'x07xA2");
        byte[] enk= Bytes.toBytes("20121201visit");
        scan.setStartRow(srk);
        scan.setStopRow(enk);
        Filter filter=new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("20121127visit")));
        scan.setFilter(filter);
        ResultScanner result=table.getScanner(scan);
        for(Result res: result){
            System.out.println(res);
            for( KeyValue kv: res.raw()){
                System.out.println("Row: "+Bytes.toString(kv.getRow())
                        +    "Key" + Bytes.toLong(kv.getKey()) + "," +
                "Value: "+Bytes.toLong(kv.getValue()));
            }
        }
    }
}
