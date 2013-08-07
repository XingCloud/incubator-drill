package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.*;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/31/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestParseRowkey {

    //private int index=0;
    private Map<String, HBaseFieldInfo> rkFieldInfoMap=new HashMap<>();
    private Map<String, Object> rkObjectMap;
    private List<KeyPart> primaryRowKeyParts;
    private List<KeyPart>[] propRowKeyParts;
    private Map<String,HBaseFieldInfo>[] propRkFieldInfoMaps;
    private String[] propertyNames={"grade","identifier","language",
                                    "last_login_time","last_pay_time"};

    private void init() throws Exception {
        String tableName="testtable_100W_deu";
        String[]projections={"event0","uid","value"};
        List<String> options=Arrays.asList(projections);
        List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, options);
        for (HBaseFieldInfo col : cols) {
            rkFieldInfoMap.put(col.fieldSchema.getName(), col);
        }
        primaryRowKeyParts=TableInfo.getRowKey(tableName,options);
        rkObjectMap=new HashMap<>();
    }

    private void initUserTable(String tableName) throws Exception{
        propRkFieldInfoMaps=new Map[propertyNames.length];
        propRowKeyParts=new List[propertyNames.length];
        for(int i=0;i<propertyNames.length;i++){
            //String[] projections={propertyNames[i],"uid"};
            String[] projections={"uid"};
            List<String> options=Arrays.asList(projections);
            List<HBaseFieldInfo> cols=TableInfo.getCols(tableName,options);
            propRkFieldInfoMaps[i]=new HashMap<>();
            for(HBaseFieldInfo col: cols){
                propRkFieldInfoMaps[i].put(col.fieldSchema.getName(),col);
            }
            propRowKeyParts[i]=TableInfo.getRowKey(tableName,options);
        }
        rkObjectMap=new HashMap<>();

    }


    @Test
    public void tesetParse()throws Exception{
        try {
            init();
            initUserTable("property_sof-dsk_index");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        long  t1,t2;
        int[] batches={1024*8,1024*64,1024*1024,1024*1024*8,1024*1024*64};

        Object[][] params = new Object[5][];
        params[0]= new Object[]{"20130312", "audit.", 1235, (byte)40};
        params[1]= new Object[]{"20190312", "audit.change.", 1235, (byte)40};
        params[2]= new Object[]{"20130312", "pay.", 1235, (byte)40};
        params[3]= new Object[]{"20130312", "visit.gross.", 1235, (byte)40};
        params[4]= new Object[]{"20130312", "a.b.c.d.e.f", 1235, (byte)40};




        Map<String, Object>[] results = new Map[5];
        for(int i=0;i<5;i++){
            results[i]=new HashMap<>();
            results[i].put("date",Integer.parseInt((String)params[i][0]));
            String[] events=((String)params[i][1]).split("\\.");
            for(int j=0;j<events.length;j++)
                results[i].put("event"+j,events[j]);
            results[i].put("uid",params[i][2]);
            results[i].put("uhash",params[i][3]);
        }

        byte[][] rks=new byte[5][];
        for(int i=0;i<5;i++){
            byte[] uidBytes=Bytes.toBytes((int)params[i][2]);
            byte[] iuid=new byte[5];
            iuid[0]=(byte)params[i][3];
            for(int j=0;j<4;j++)iuid[j+1]=uidBytes[j];
            String event=(String)params[i][1];
            byte[] date=Bytes.toBytes((String)params[i][0]);
            rks[i]=HBaseEventUtils.getRowKey(date,event,iuid);
        }


        DFARowKeyParser dfaParser=new DFARowKeyParser(primaryRowKeyParts,rkFieldInfoMap);
        for(int j=0;j<batches.length;j++){
            t1=System.currentTimeMillis();
            for(int k=0;k<batches[j];k++){
              for (int i = 0; i < params.length; i++) {
                parseRowKey(rks[i],dfaParser,results[i]);
              }
            }
            t2=System.currentTimeMillis();
            System.out.println((t2-t1)+" ms parse kv "+batches[j]*params.length);
        }

    }
    @Test
    public void testParseUser(){
        try {
            initUserTable("property_sof-dsk_index");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        long  t1,t2;
        int[] batches={1024*8,1024*64,1024*1024,1024*1024*8,1024*1024*16};

        Object[][] userParams=new Object[5][];
        userParams[0]=new Object[]{(short)1,20120210,12l};
        userParams[1]=new Object[]{(short)2,20120210,"tr"};
        userParams[2]=new Object[]{(short)3,20120210,"en"};
        userParams[3]=new Object[]{(short)4,20120210,13000012222222l};
        userParams[4]=new Object[]{(short)5,20120210,13000000122222l};

        Map<String,Object>[] userResults=new Map[5];
        for(int i=0;i<5;i++){
            userResults[i]=new HashMap<>();
            userResults[i].put("propnumber",userParams[i][0]);
            userResults[i].put("date",userParams[i][1]);
            userResults[i].put(propertyNames[i],userParams[i][2]);
        }

        byte[][] rks=new byte[5][];
        for(int i=0;i<5;i++){
            byte[] propId=Bytes.toBytes((short)userParams[i][0]);
            byte[] date=Bytes.toBytes(String.valueOf(userParams[i][1]));
            byte[] val;
            if(userParams[i][2] instanceof String)
                val=Bytes.toBytes((String)userParams[i][2]);
            else
                val=Bytes.toBytes((long)userParams[i][2]);
            rks[i]=HBaseUserUtils.getRowKey(propId,date,val);
        }

        DFARowKeyParser[] parsers=new DFARowKeyParser[5];
        for(int i=0;i<5;i++)
            parsers[i]=new DFARowKeyParser(propRowKeyParts[i],propRkFieldInfoMaps[i]);

        for(int j=0;j<batches.length;j++){
            t1=System.currentTimeMillis();
            for(int k=0;k<batches[j];k++){
                for (int i = 0; i < userParams.length; i++) {
                    parseRowKey(rks[i],parsers[i],userResults[i]);
                }
            }
            t2=System.currentTimeMillis();
            System.out.println((t2-t1)+" ms parse kv "+batches[j]*userParams.length);
        }

    }

    public void parseUserRowKey(short propId,int date,Object val,DFARowKeyParser dfaParser,
                                Map<String,Object> refResults){
        byte[] propIdBytes=Bytes.toBytes(propId);
        byte[] dateBytes=Bytes.toBytes(String.valueOf(date));
        byte[] valBytes;
        if(val instanceof String)
            valBytes=Bytes.toBytes((String)val);
        else valBytes=Bytes.toBytes((long)val);
        byte[] rk= HBaseUserUtils.getRowKey(propIdBytes,dateBytes,valBytes);
        List<KeyPart> rkParts=propRowKeyParts[propId-1];
        Map<String,HBaseFieldInfo> rkFieldInfoMap=propRkFieldInfoMaps[propId-1];
        //Map<String,Object> parsedResult=RowKeyParser.parse(rk,rkParts,rkFieldInfoMap);
        Map<String,Object> parsedResult= dfaParser.parse(rk);
        for(Map.Entry<String,Object> entry: refResults.entrySet()){
            Object o=parsedResult.get(entry.getKey());
            if(null==o)System.out.println(entry.getKey()+":"+entry.getValue());
            assert o.equals(entry.getValue());
        }
    }

    public void parseRowKey(String day, String event, int uid, byte uhash, DFARowKeyParser dfaParser,Map<String, Object> result){
        byte[] uidBytes=Bytes.toBytes(uid);
        byte[] iuid=new byte[5];
        iuid[0]=uhash;
        for(int i=0;i<4;i++)iuid[i+1]=uidBytes[i];
        //byte[] rk= HBaseEventUtils.getRowKey(Bytes.toBytes(day),event,iuid);
        byte[] rk=new byte[event.length()+14];
        byte[] date=Bytes.toBytes(day);
        int index = 0;

        for (int i = 0; i < date.length; i++) {
            rk[index++] = date[i];
        }
        for (int i = 0; i < event.length(); i++) {
            rk[index++] = (byte) (event.charAt(i) & 0xFF);
        }

        //uid前加上oxff
        rk[index++] = (byte) 0xff;

        for (int i = 0; i < iuid.length; i++) {
            rk[index++] = iuid[i];
        }
        Map<String,Object> parsedResult= dfaParser.parse(rk);
        /*for(Map.Entry<String,Object> entry: result.entrySet()){
            Object o=parsedResult.get(entry.getKey());
            if(null==o)System.out.println(event);
            assert o.equals(entry.getValue());
        }*/
    }

    public void parseRowKey(byte[] rk,DFARowKeyParser dfaParser,Map<String, Object> result){
        Map<String,Object> parsedResult= dfaParser.parse(rk);
        for(Map.Entry<String,Object> entry: result.entrySet()){
            Object o=parsedResult.get(entry.getKey());
            if(null==o){
                //System.out.println(entry.getKey());
                continue;
            }
            assert o.equals(entry.getValue());
        }
    }




}
