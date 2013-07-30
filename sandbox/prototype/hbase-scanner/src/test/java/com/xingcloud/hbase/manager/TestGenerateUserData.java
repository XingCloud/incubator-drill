package com.xingcloud.hbase.manager;

import com.xingcloud.hbase.util.HBaseUserUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/26/13
 * Time: 7:34 PM
 * To change this template use File | Settings | File Templates.
 */


public class TestGenerateUserData {
      public static Logger logger= org.slf4j.LoggerFactory.getLogger(TestGenerateUserData.class);
      private Map<String,List<Object>> propertyValue=new HashMap<>();

    @Test
    public  void createTable10W(){
        GenerateData(1000*100);
    }
    @Test
    public void generateData100W(){
        GenerateData(1000*1000);
    }
    @Test
    public  void generateData10000W(){
        GenerateData(1000*1000*100);
    }
    @Test
    public void test(){
        System.out.println("hh");
    }
      private void GenerateData(int batch){
          Configuration conf= HBaseConfiguration.create();
          HBaseAdmin admin = null;
          int num=batch/10000;
          String tableName="property_"+"testtable_"+num+"W_"+"index";


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
              table.setWriteBufferSize(1024*1024*10);
              List<Properties> propertiesList=getProperties();
              String[] days={"20121201","20121202","20121203","20121204","20121205"};
              int forUnit=(batch/days.length)/propertiesList.size();
              table.setAutoFlush(false);
              int cache=1000*60;
              long t1=System.currentTimeMillis();
              long pret=System.currentTimeMillis();
              long post=pret;
              List<Put> puts=new ArrayList<>();
              Put put=null;
              for(int i=0;i< days.length;i++){
                  for(int l=0;l<propertiesList.size();l++){
                      Properties prop=propertiesList.get(l);
                      int uidSeed=batch/(10*days.length);
                      Random uidR=new Random(uidSeed);
                      short propId=(short)prop.getId();
                      List<Object> Values=propertyValue.get(prop.getPropertyName());
                      if(Values.size()==0)continue;
                      int valueSize=Values.size();
                      for(int j=0;j<Values.size();j++){
                          Object o=Values.get(j);
                          byte[] valBytes=null;
                          long propTypeT1=System.currentTimeMillis();
                          switch (prop.getType()){
                              case bigint:
                              case datetime:
                                  valBytes=Bytes.toBytes((long)o);
                                  break;
                              case string:
                                  valBytes=Bytes.toBytes((String)o);
                                  break;
                              default:
                                  break;
                          }
                          long propTypeT2=System.currentTimeMillis();
                          System.out.println("get propVal "+ (propTypeT2-propTypeT1)+" ms");

                          for(int k=0;k<forUnit/valueSize;k++){
                              long byTimet1=System.currentTimeMillis();
                              int uid=uidR.nextInt(uidSeed);
                              byte[] uidBytes=Bytes.toBytes(uid);
                              byte[] dayBytes=Bytes.toBytes(days[i]);
                              byte[] propIdBytes=Bytes.toBytes(propId);
                              byte[] hbaseValue=Bytes.toBytes(false);
                              byte[] rk= HBaseUserUtils.getRowKey(propIdBytes, dayBytes, valBytes);
                              long byTimet2=System.currentTimeMillis();
                              System.out.println("get bytes "+(byTimet2-byTimet1)+" ms");
                              if(k%50==0)
                              {
                                  put=new Put(rk);
                                  put.setWriteToWAL(false);
                              }
                              put.add(Bytes.toBytes("val"), uidBytes, hbaseValue);
                              if(k%1000==0)
                                puts.add(put);
                              if(puts.size()==10)
                              {
                                  table.put(puts);
                                  puts=new ArrayList<>();
                              }
                              long t3=System.currentTimeMillis();
                              System.out.println("put to table "+(t3-byTimet2)+" ms");
                              if(t3-byTimet2>10)System.out.println("too long time");
                              recordNum++;
                              if(recordNum%cache==0){
                                  System.out.println(recordNum);
                                  pret=System.currentTimeMillis();
                                  System.out.println("plus commit"+ (pret-post)+" ms");
                                  table.flushCommits();
                                  post=System.currentTimeMillis();
                                  System.out.println("commit"+(post-pret)+" ms");
                              }
                          }
                          //System.out.println("create record num "+forUnit/valueSize+ "for day "+
                          //                    days[i]+ " prop "+prop.getPropertyName()+" value "+o);
                      }
                      //System.out.println("create record num "+forUnit+ "for day "+
                      //        days[i]+ " prop "+prop.getPropertyName());

                  }
              }
              table.flushCommits();

              long t2=System.currentTimeMillis();
              System.out.println("generate data into hbase costing "+(t2-t1)+"ms");
              System.out.println("create record num "+recordNum);
              for(int i=0;i<days.length;i++){
                  System.out.println("create record num "+recordNum/days.length+ "for day "+days[i]);
              }
          } catch (IOException e) {
              System.out.println("recordNum "+recordNum);
              e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
      }
    private List<Properties> getProperties() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        HTable table=new HTable(conf,"properties_sof-dsk");
        Scan scan=new Scan();
        ResultScanner scanner=table.getScanner(scan);

        List<Properties> propertiesList=new ArrayList<>();
        for(Result r: scanner){
            byte[] rk=r.getRow();
            Properties properties=new Properties();
            String propertyName=Bytes.toString(rk);
            properties.setPropertyName(propertyName);
            for(KeyValue kv: r.raw()){
                 byte[] qualifier=kv.getQualifier();
                 String quaName=Bytes.toString(qualifier);
                 switch (quaName){
                     case "func":
                           String funcName=Bytes.toString(kv.getValue());
                           Func func=Func.getFunc(funcName);
                           properties.setFunc(func);
                           break;
                     case "id":
                           int id=Bytes.toInt(kv.getValue());
                           properties.setId(id);
                           break;
                     case "type":
                           String typeName=Bytes.toString(kv.getValue());
                           Type type=Type.getType(typeName);
                           properties.setType(type);
                           break;
                     case "orig":
                           String origName=Bytes.toString(kv.getValue());
                           Orig orig=Orig.getOrig(origName);
                           properties.setOrig(orig);
                           break;
                 }
            }
            propertiesList.add(properties);
        }
        SimpleDateFormat df=new SimpleDateFormat("yyyyMMddHHMMss");
        for(Properties prop: propertiesList){
           switch (prop.getPropertyName()){
               case "coin_buy":
               case "coin_promotion":
               case "coin_initialstatus":
                    List<Object> coinVals=new ArrayList<>();
                    coinVals.add(100l);
                    coinVals.add(1000l);
                    coinVals.add(10000l);
                    propertyValue.put(prop.getPropertyName(),coinVals);
                    break;
               case "ref":
                   List<Object> refVals=new ArrayList<>();
                   refVals.add("muh");
                   refVals.add("gzg");
                   refVals.add("slbnew");
                   refVals.add("idd");
                   propertyValue.put(prop.getPropertyName(),refVals);
                   break;
               case "ref0":
                   List<Object> refiVals=new ArrayList<>();
                   refiVals.add("gpad");
                   refiVals.add("cor");
                   refiVals.add("meg");
                   refiVals.add("tugs");
                   propertyValue.put(prop.getPropertyName(),refiVals);
                   break;
               case "ref1":
               case "ref2":
               case "ref3":
               case "ref4":
                   List<Object> refNullVals=new ArrayList<>();
                   propertyValue.put(prop.getPropertyName(),refNullVals);
                   break;
               case "first_pay_time":
               case "register_time":
                   List<Object> timeFstVals=new ArrayList<>();
                   try {
                       timeFstVals.add(df.parse("20120101003000").getTime());
                       timeFstVals.add(df.parse("20120101120000").getTime());
                       timeFstVals.add(df.parse("20120102000000").getTime());
                   } catch (ParseException e) {
                       e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                   }
                   propertyValue.put(prop.getPropertyName(),timeFstVals);
                   break;
               case "last_pay_time":
               case "last_login_time":
                   List<Object> timeLstVals=new ArrayList<>();
                   try {
                       timeLstVals.add(df.parse("20121201120030").getTime());
                       timeLstVals.add(df.parse("20121203000000").getTime());
                       timeLstVals.add(df.parse("20121204053000").getTime());
                   } catch (ParseException e) {
                       e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                   }
                   propertyValue.put(prop.getPropertyName(),timeLstVals);
                   break;
               case "geoip":
                   List<Object> geoIpVals=new ArrayList<>();
                   geoIpVals.add("tr");
                   geoIpVals.add("ar");
                   geoIpVals.add("br");
                   propertyValue.put(prop.getPropertyName(),geoIpVals);
                   break;
               case "grade":
                   List<Object> gradeVals=new ArrayList<>();
                   gradeVals.add(new Long(112));
                   gradeVals.add(new Long(124));
                   gradeVals.add(new Long(251));
                   propertyValue.put(prop.getPropertyName(),gradeVals);
                   break;
               case "identifier":
                   List<Object> identifierVals=new ArrayList<>();
                   identifierVals.add("1qu");
                   identifierVals.add("farmorkut");
                   propertyValue.put(prop.getPropertyName(),identifierVals);
                   break;
               case "platform":
                   List<Object> platformVals=new ArrayList<>();
                   platformVals.add("meinvz");
                   platformVals.add("orkut");
                   propertyValue.put(prop.getPropertyName(),platformVals);
                   break;
               case "language":
                   List<Object> languageVals=new ArrayList<>();
                   languageVals.add("tw");
                   languageVals.add("de");
                   propertyValue.put(prop.getPropertyName(),languageVals);
                   break;
               case "pay_amount":
                   List<Object> payAmtVals=new ArrayList<>();
                   payAmtVals.add(new Long(56));
                   payAmtVals.add(new Long(134));
                   payAmtVals.add(new Long(435));
                   propertyValue.put(prop.getPropertyName(),payAmtVals);
               case "nation":
               case "version":
                   List<Object> NullVals=new ArrayList<>();
                   propertyValue.put(prop.getPropertyName(),NullVals);
                   break;
           }
        }
        return propertiesList;
    }

    public enum Func{
        inc("inc"),
        cover("cover"),
        empty("empty");
        private String func;
        private Func(String func){
            this.func=func;
        }
        public  void setFunc(String func){
            this.func=func;
        }
        public static Func getFunc(String funcName){
            switch (funcName){
                case "inc":
                    return Func.inc;
                case "cover":
                    return Func.cover;
            }
            return Func.empty;
        }

    }
    public enum Orig{
        sys("sys"),empty("empty");
        private String orig;
        private Orig(String orig){
            this.orig=orig;
        }
        public void setOrig(String orig){
            this.orig=orig;
        }
        public static Orig getOrig(String origName){
            switch (origName){
                case "sys":
                    return Orig.sys;
            }
            return Orig.empty;
        }

    }

    public enum Type{
        bigint("sql_bigint"),datetime("sql_datetime"),string("sql_string"),empty("empty");
        private String type;
        private Type(String type){
            this.type=type;
        }
        public void setType(String type){
            this.type=type;
        }
        public static Type getType(String typeName){
            switch (typeName){
                case "sql_bigint":
                    return bigint;
                case "sql_datetime":
                    return datetime;
                case "sql_string":
                    return string;
                default:
                    return empty;
            }
        }

    }

    public static class Properties{
        private String propertyName;
        private int id;
        private Func func;
        private Orig orig;
        private Type type;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public Func getFunc() {
            return func;
        }

        public void setFunc(Func func) {
            this.func = func;
        }

        public Orig getOrig() {
            return orig;
        }

        public void setOrig(Orig orig) {
            this.orig = orig;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public void setPropertyName(String propertyName) {
            this.propertyName = propertyName;
        }




    }


}
