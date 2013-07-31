package com.xingcloud.meta;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultDrillHiveMetaClient extends HiveMetaStoreClient {
  public DefaultDrillHiveMetaClient(HiveConf conf) throws MetaException {
    super(conf);
  }

  public DefaultDrillHiveMetaClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
    super(conf, hookLoader);
  }

  /**
   * make life easier when creating tables
   * @param tbl
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  @Override
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    StorageDescriptor sd = tbl.getSd();
    if(sd.getSkewedInfo() == null){
      sd.setSkewedInfo(new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(), new HashMap<List<String>, String>()));
    }
    if(sd.getSerdeInfo() == null){
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      Map<String, String> parameters = new HashMap<String, String>();
      parameters.put("serialization.format", "1");
      serdeInfo.setParameters(parameters);
      sd.setSerdeInfo(serdeInfo);
    }
    if(sd.getLocation() == null){
      sd.setLocation("");      
    }
    if(sd.getInputFormat() == null){
      sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    }
    if(sd.getOutputFormat() == null){
      sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    }
    if(sd.getBucketCols() == null){
      sd.setBucketCols(new ArrayList<String>());
    }
    if(sd.getSortCols() == null){
     sd.setSortCols(new ArrayList<Order>());
    }
    if(sd.getParameters() == null){
      sd.setParameters(new HashMap<String, String>());
    }
    if(tbl.getPartitionKeys()==null){
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }
    if(tbl.getTableType()==null){
      tbl.setTableType("EXTERNAL_TABLE");      
    }
    super.createTable(tbl);    
  }

  public static Table GetTable(String tableName,List<String> options) throws Exception{
    HiveConf conf=new HiveConf();
    HiveMetaStoreClient client=new HiveMetaStoreClient(conf);
    Table table=client.getTable("test_xa",tableName);
    if(tableName.endsWith("index")){
        String regPropTableName="register_template_prop_index";
        Table regPropTable=client.getTable("test_xa",regPropTableName);
        List<FieldSchema> fieldSchemas=regPropTable.getSd().getCols();
        for(int i=0;i<fieldSchemas.size();i++){
            if(options.contains(fieldSchemas.get(i).getName())){
               HBaseFieldInfo info=HBaseFieldInfo.getColumnType(regPropTable,fieldSchemas.get(i));
               String primaryRK=TableInfo.getPrimaryKeyPattern(table);
               primaryRK+="${"+fieldSchemas.get(i).getName()+"}";
               table.getSd().addToCols(fieldSchemas.get(i));
               HBaseFieldInfo.setColumnType(table,fieldSchemas.get(i),info.fieldType,info.cfName,
                                            info.cqName,info.serType,info.serLength);
               TableInfo.setPrimaryKeyPattern(table,primaryRK);
               break;
            }
        }

    }
    return table;
  }
  
}
