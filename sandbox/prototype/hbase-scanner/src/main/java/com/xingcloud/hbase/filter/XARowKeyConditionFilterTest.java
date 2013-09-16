package com.xingcloud.hbase.filter;

import com.xingcloud.xa.hbase.filter.XARowKeyPatternFilter;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterCondition;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterPattern;
import com.xingcloud.xa.hbase.util.rowkeyCondition.RowKeyFilterRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/12/13
 * Time: 7:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class XARowKeyConditionFilterTest {
    public static Logger logger= LoggerFactory.getLogger(XARowKeyConditionFilterTest.class);
     public static void main() throws IOException {

         byte[] srk= Bytes.toBytesBinary("20121201");
         byte[] enk=Bytes.toBytesBinary("20130103visit.\\xFF");

         List<RowKeyFilterCondition> filterConditionList=new ArrayList<>();
         String uidSrt= Bytes.toStringBinary(Bytes.tail(Bytes.toBytes(0l),5));
         String uidEnd= Bytes.toStringBinary(new byte[]{-1,-1,-1,-1,-1});
         //filterConditionList.add(new RowKeyFilterPattern("20121201visit.auto.\\xFF",uidSrt,uidEnd));
         filterConditionList.add(new RowKeyFilterRange("20121201z","20130102d",uidSrt,uidEnd));
         //filterConditionList.add(new RowKeyFilterRange("20130102z","20130103b"));
         //filterConditionList.add(new RowKeyFilterRange("20130102v","20130102y"));
         //filterConditionList.add(new RowKeyFilterRange("20130101visit.s","20130101visit.y") );
         //filterConditionList.add(new RowKeyFilterRange("20130102","20130102visit.y"));
         XARowKeyPatternFilter filter=new XARowKeyPatternFilter(filterConditionList);

         long t1=System.currentTimeMillis();
         int count=0;
         DirectScanner scanner=new DirectScanner(srk,enk,"sof-dsk_deu",filter,false,false);
         List<KeyValue> kvs=new ArrayList<KeyValue>();
         while(scanner.next(kvs)){
             count+=kvs.size();
         }
         /*
         Configuration conf= HBaseConfiguration.create();
         HTable table=new HTable(conf,"sof-dsk_deu");
         Scan scan=new Scan();
         scan.setStartRow(srk);
         scan.setStopRow(enk);
         scan.setFilter(filter);
         scan.setBatch(10);
         scan.setCaching(2048);
         ResultScanner resultScanner=table.getScanner(scan);
         logger.info("hhhh");

         for(Result res: resultScanner){
             for(KeyValue kv :res.raw()){
                 //logger.info(kv.toString());
                 count++;
             }
         }
         */
         long t2=System.currentTimeMillis();
         logger.info("use time "+(t2-t1)+" count "+count);
     }
}
