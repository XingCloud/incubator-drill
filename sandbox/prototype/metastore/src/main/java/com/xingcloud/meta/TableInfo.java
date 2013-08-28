package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableInfo {
  public static final String PRIMARY_KEY = "primarykey.columns";
  public static final String PK_DELIMITER = "primarykey.delimiter";
  public static final String STORAGE_ENGINE = "drill.se";
  
  public static final String SE_HBASE = "hbase";
  
  
  public static List<KeyPart> getPrimaryKey(Table table) {
    String pkString = getPrimaryKeyPattern(table); 
    if(pkString == null || "".equals(pkString)){
      return null;
    }
    return PrimaryKeyPattern.parse(table, pkString);
  }


  public static String getPrimaryKeyPattern(Table table) {
    return table.getSd().getSerdeInfo().getParameters() == null? null: table.getSd().getSerdeInfo().getParameters().get(PRIMARY_KEY);    
  }
  
  
  public static void setPrimaryKeyPattern(Table table, String primaryKeyPattern) {
    String oldKey = table.getSd().getSerdeInfo().getParameters().get(PRIMARY_KEY);
    table.getSd().getSerdeInfo().putToParameters(PRIMARY_KEY, primaryKeyPattern);
    try{
      getPrimaryKey(table);
    }catch(Exception e){
      table.getSd().getSerdeInfo().putToParameters(PRIMARY_KEY, oldKey);     
      throw new RuntimeException(e);
    }
  }

    public static List<HBaseFieldInfo> getCols(String tableName,List<String> options) throws Exception {
        Table table= getTable(tableName, options);
        List<HBaseFieldInfo> colFieldInfoList=new ArrayList<HBaseFieldInfo>();
        for(FieldSchema fieldSchema: table.getSd().getCols()){
            HBaseFieldInfo fieldInfo = HBaseFieldInfo.getColumnType(table, fieldSchema);
            if(fieldInfo!=null)colFieldInfoList.add(fieldInfo);
        }
        return colFieldInfoList;
    }

    public static List<HBaseFieldInfo> getPrimaryKey(String tableName,List<String> options) throws Exception{
        List<HBaseFieldInfo> colInfos=getCols(tableName,options);
        Table table= getTable(tableName, options);
        List<KeyPart> pkSequence = TableInfo.getPrimaryKey(table);
        List<HBaseFieldInfo> ret=new ArrayList<HBaseFieldInfo>();
        for(KeyPart kp: pkSequence){
            for(HBaseFieldInfo col: colInfos){
                if(kp.getField()!=null&&kp.getField().getName().equals(col.fieldSchema.getName()))
                    ret.add(col);
            }
        }
        return ret;
    }

    public static List<KeyPart> getRowKey(String tableName,List<String> options) throws Exception{
        Table table= getTable(tableName, options);
        List<KeyPart> pkSequence = TableInfo.getPrimaryKey(table);
        return pkSequence;
    }



    public static void setPrimaryKey(Table table, List<FieldSchema> fieldSchemas) {
    StringBuilder sb = new StringBuilder();
    if (fieldSchemas != null){
      for (int i = 0; i < fieldSchemas.size(); i++) {
        FieldSchema fieldSchema = fieldSchemas.get(i);
        sb.append(fieldSchema.getName());
        if(i != fieldSchemas.size() - 1){
          sb.append(",");
        }
      }
    }
    table.getSd().getSerdeInfo().putToParameters(PRIMARY_KEY, sb.toString());    
  }
  
  public static void setStorageEngine(Table table, String se){
    table.getParameters().put(STORAGE_ENGINE, se);
  }
  
  public static String getStorageEngine(Table table){
    return table.getParameters().get(STORAGE_ENGINE);
  }
  
  public static Table newTable() {
    Table table2 = new Table();
    StorageDescriptor sd = new StorageDescriptor();
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    Map<String, String> parameters = new HashMap<String, String>();
    parameters.put("serialization.format", "1");
    serdeInfo.setParameters(parameters);
    SkewedInfo skewedInfo = new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(), new HashMap<List<String>, String>());
    sd.setSkewedInfo(skewedInfo);
    sd.setSerdeInfo(serdeInfo);
    sd.setCols(new ArrayList<FieldSchema>());
//    sd.setLocation("hdfs://localhost:9000/user/hive/warehouse/test_xa.db/");// need a place
    sd.setLocation("");// need a place
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");//y
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");//y
    sd.setBucketCols(new ArrayList<String>());
    sd.setSortCols(new ArrayList<Order>());
    sd.setParameters(new HashMap<String, String>());
    table2.setSd(sd);//y
    table2.setPartitionKeys(new ArrayList<FieldSchema>());//y
    table2.setTableType("EXTERNAL_TABLE");
    
    Map<String, String> table2Params = new HashMap<String, String>();
//    table2Params.put("transient_lastDdlTime", "" + System.currentTimeMillis());
    table2.setParameters(table2Params);
    return table2;
  }

  public static Table getTable(String tableName, List<String> options) throws Exception {
    DrillHiveMetaClient client = ProxyMetaClientFactory.getInstance().newProxiedPooledClient();
    Table table = client.getTable(tableName);
    String regPropTableName;
    if (tableName.startsWith("mysql"))
      regPropTableName = "mysql_register_template_prop";
    else
      regPropTableName = "register_template_prop_index";

    if (tableName.contains("property")) {
      Table regPropTable = client.getTable(regPropTableName);
      List<FieldSchema> fieldSchemas = regPropTable.getSd().getCols();
      for (int i = 0; i < fieldSchemas.size(); i++) {
        if (options!=null&&options.contains(fieldSchemas.get(i).getName())) {
          HBaseFieldInfo info = HBaseFieldInfo.getColumnType(regPropTable, fieldSchemas.get(i));
          String primaryRK = getPrimaryKeyPattern(table);
          primaryRK += "${" + fieldSchemas.get(i).getName() + "}";
          table.getSd().addToCols(fieldSchemas.get(i));
          HBaseFieldInfo
            .setColumnType(table, fieldSchemas.get(i), info.fieldType, info.cfName, info.cqName, info.serType,
              info.serLength);
          setPrimaryKeyPattern(table, primaryRK);
          break;
        }
      }

    }
    return table;
  }
}
