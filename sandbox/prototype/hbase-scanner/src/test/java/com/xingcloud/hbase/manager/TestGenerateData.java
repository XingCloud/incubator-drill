package com.xingcloud.hbase.manager;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/15/13
 * Time: 6:17 PM
 * To change this template use File | Settings | File Templates.
 */

import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Random;

//import javax.security.auth.login.Configuration;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/15/13
 * Time: 2:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestGenerateData {
    public static Logger logger = org.slf4j.LoggerFactory.getLogger(TestGenerateData.class);
    @BeforeClass
    public static void createTable10W(){
        generateData(1000*100);
    }
    @BeforeClass
    public static void generateData100W(){
        generateData(1000*1000);
    }
    @AfterClass
    public static void generateData10000W(){
        generateData(1000*1000*100);
    }
    @Test
    public void test(){
        System.out.println("hh");
    }
    private static void generateData(int batch){
        Configuration conf= HBaseConfiguration.create();
        HBaseAdmin admin = null;
        int num=batch/10000;
        String tableName="testtable"+num+"W_deu";


        try {
            admin = new HBaseAdmin(conf);
            HTableDescriptor desc=new HTableDescriptor(Bytes.toBytes(tableName));
            HColumnDescriptor coldef=new HColumnDescriptor(Bytes.toBytes("val"));
            desc.addFamily(coldef);
            boolean avail=admin.isTableAvailable(Bytes.toBytes(tableName));
            if(avail){
                if(admin.isTableEnabled(Bytes.toBytes(tableName)))
                    admin.disableTableAsync(Bytes.toBytes(tableName));
                while(!admin.isTableDisabled(Bytes.toBytes(tableName))){

                }
                if(admin.tableExists(Bytes.toBytes(tableName)))
                    admin.deleteTable(Bytes.toBytes(tableName));
                System.out.println("table "+ tableName+" already exists!");
            }
            admin.createTable(desc);
            avail=admin.isTableAvailable(Bytes.toBytes(tableName));
            System.out.println("Table available: "+avail);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }catch (IOException e){
            e.printStackTrace();
        }

        int recordNum=0;
        try {
            HTable table=new HTable(conf,tableName);
            String[] events={"ad","visit","login","pay","consume"};
            double[] ratios={0.2,0.3,0.1,0.25,0.15};
            String[] days={"20121201","20121202","20121203","20121204","20121205"};
            int forUnit=(batch/days.length);
            table.setAutoFlush(false);
            int cache=1000*10;
            long t1=System.currentTimeMillis();

            for(int i=0;i< days.length;i++){
                for(int j=0;j<events.length;j++){
                    Random uidR=new Random(batch/(10*days.length));
                    Random val=new Random(10000l);
                    for(int k=0;k<forUnit*ratios[j];k++){
                        int uid=uidR.nextInt();
                        int value=val.nextInt();
                        byte[] uidBytes=Bytes.toBytes(uid);
                        byte[] valBytes=Bytes.toBytes(value);
                        byte[] dayBytes=Bytes.toBytes(days[i]);
                        byte[] rk= HBaseEventUtils.getRowKey(dayBytes, events[j], uidBytes);
                        Put put=new Put(rk);
                        put.add(Bytes.toBytes("val"),Bytes.toBytes("val"),valBytes);
                        //puts.add(put);
                        table.put(put);
                        recordNum++;
                        if(recordNum%cache==0){
                            System.out.println(recordNum);
                            table.flushCommits();
                        }
                    }
                }
            }
            table.flushCommits();

            long t2=System.currentTimeMillis();
            System.out.println("generate data into hbase costing "+(t2-t1)+"ms");
            System.out.println("create record num "+recordNum);
            for(int i=0;i<events.length;i++){
                System.out.println("create record num "+recordNum*ratios[i]+ "for event "+events[i]);
            }
            for(int i=0;i<days.length;i++){
                System.out.println("create record num "+recordNum/days.length+ "for day "+days[i]);
            }
        } catch (IOException e) {
            System.out.println("recordNum "+recordNum);
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }





}
