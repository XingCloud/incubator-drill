package com.xingcloud.meta;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultDrillHiveMetaClient2 {
  public static final String DEFAULT_DB_NAME = "test_xa";
  private static DefaultDrillHiveMetaClient2 instance;

  private DefaultDrillHiveMetaClient2() throws MetaException {
    hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
  }

  public static synchronized DefaultDrillHiveMetaClient2 getInstance() throws MetaException {
    if (instance == null) {
      instance = new DefaultDrillHiveMetaClient2();
    }
    return instance;
  }

  private HiveMetaStoreClient hiveMetaStoreClient;

  public void createTable(Table tbl) throws TException {
    StorageDescriptor sd = tbl.getSd();
    if (sd.getSkewedInfo() == null) {
      sd.setSkewedInfo(
        new SkewedInfo(new ArrayList<String>(), new ArrayList<List<String>>(), new HashMap<List<String>, String>()));
    }
    if (sd.getSerdeInfo() == null) {
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      Map<String, String> parameters = new HashMap<String, String>();
      parameters.put("serialization.format", "1");
      serdeInfo.setParameters(parameters);
      sd.setSerdeInfo(serdeInfo);
    }
    if (sd.getLocation() == null) {
      sd.setLocation("");
    }
    if (sd.getInputFormat() == null) {
      sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    }
    if (sd.getOutputFormat() == null) {
      sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    }
    if (sd.getBucketCols() == null) {
      sd.setBucketCols(new ArrayList<String>());
    }
    if (sd.getSortCols() == null) {
      sd.setSortCols(new ArrayList<Order>());
    }
    if (sd.getParameters() == null) {
      sd.setParameters(new HashMap<String, String>());
    }
    if (tbl.getPartitionKeys() == null) {
      tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    }
    if (tbl.getTableType() == null) {
      tbl.setTableType("EXTERNAL_TABLE");
    }
    hiveMetaStoreClient.createTable(tbl);
  }

  public Table getTable(String tableName) throws TException {
    return getTable(DEFAULT_DB_NAME, tableName);
  }

  public Table getTable(String dbName, String tableName) throws TException {
//    return null;
    if (tableName.contains("-"))
      tableName = tableName.replaceAll("-", "Mns");
    if (tableName.startsWith("deu_") || tableName.endsWith("_deu"))
      tableName = "eventTableMeta";
    return hiveMetaStoreClient.getTable(dbName, tableName);
  }

  public Table getTable(String tableName, List<String> options) throws TException {
    Table table = getTable(tableName);
    String regPropTableName;
    if (tableName.startsWith("mysql"))
      regPropTableName = "mysql_register_template_prop";
    else
      regPropTableName = "register_template_prop_index";

    if (tableName.contains("property")) {
      Table regPropTable = getTable(regPropTableName);
      List<FieldSchema> fieldSchemas = regPropTable.getSd().getCols();
      for (int i = 0; i < fieldSchemas.size(); i++) {
        if (options.contains(fieldSchemas.get(i).getName())) {
          HBaseFieldInfo info = HBaseFieldInfo.getColumnType(regPropTable, fieldSchemas.get(i));
          String primaryRK = TableInfo.getPrimaryKeyPattern(table);
          primaryRK += "${" + fieldSchemas.get(i).getName() + "}";
          table.getSd().addToCols(fieldSchemas.get(i));
          HBaseFieldInfo
            .setColumnType(table, fieldSchemas.get(i), info.fieldType, info.cfName, info.cqName, info.serType,
                           info.serLength);
          TableInfo.setPrimaryKeyPattern(table, primaryRK);
          break;
        }
      }

    }
    return table;
  }

}
