package com.xingcloud.xa.meta;

import com.xingcloud.meta.DefaultDrillHiveMetaClient;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.List;

public class TestDefaultDrillHiveMetaClient {

  @Test
  public void testCreateDEUTable() throws Exception{
    String tableName = "testtable100W_deu";
    String dbName = "test_xa";
    String userTableName = "user_sofMnsdsk";
    String userIndexName = "property_sofMnsdsk_index";
    String userRegPropName="register_template_prop_index";
    //createDEUTable(tableName);
    createUserTable(userTableName);
    createUserIndex(userIndexName);
    //createPropRegisterTable();

    DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
    Table table = client.getTable(dbName, tableName);
    printColumns(table);
    printColumns(client.getTable(dbName, userTableName));
    printColumns(client.getTable(dbName, userIndexName));
    printColumns(client.getTable(dbName,userRegPropName));
    
  }

  private void printColumns(Table table) {
    List<KeyPart> pkSequence = TableInfo.getPrimaryKey(table);
    System.out.println("pkSequence = " + pkSequence);
    for(FieldSchema fieldSchema: table.getSd().getCols()){
      HBaseFieldInfo fieldInfo = HBaseFieldInfo.getColumnType(table, fieldSchema);
      System.out.println("fieldInfo = " + fieldInfo);
    }
  }

  private void createPropRegisterTable() throws TException {
       String[] propName={"coin_buy","coin_promotion","coin_initialstatus","grade","pay_amount",
                          "first_pay_time","register_time","last_login_time","last_pay_time",
                           "nation","geoip","identifier","platform","language","version",
                           "ref","ref0","ref1","ref2","ref3","ref4"};
       String tableName="register_template_prop_index";
       String dbName="test_xa";
       HiveConf conf = new HiveConf();
       DefaultDrillHiveMetaClient client = new DefaultDrillHiveMetaClient(conf);
       Table regPropIndex = TableInfo.newTable();
       regPropIndex.setDbName(dbName);
       regPropIndex.setTableName(tableName);
       client.dropTable(dbName, tableName);
       for(int i=0;i<21;i++){
           if(i<9){
               FieldSchema field=new FieldSchema(propName[i],"bigint","BINARY:8");
               regPropIndex.getSd().addToCols(field);
               HBaseFieldInfo.setColumnType(regPropIndex,field, HBaseFieldInfo.FieldType.rowkey,null,null,
                       HBaseFieldInfo.DataSerType.BINARY,8);
           }
           else{
              FieldSchema field=new FieldSchema(propName[i],"string","TEXT");
              regPropIndex.getSd().addToCols(field);
              HBaseFieldInfo.setColumnType(regPropIndex,field, HBaseFieldInfo.FieldType.rowkey,null,null,
                      HBaseFieldInfo.DataSerType.WORD,0);
           }
       }
       client.createTable(regPropIndex);
  }

  private void createDEUTable(String table) throws TException {
      HiveConf conf = new HiveConf();
      DefaultDrillHiveMetaClient client = new DefaultDrillHiveMetaClient(conf);
      Table deu = TableInfo.newTable();
      String dbName = "test_xa";
      deu.setTableName(table);
      deu.setDbName(dbName);
      client.dropTable(dbName, table);
      FieldSchema dateField = new FieldSchema("date", "int", "TEXT:8");
      HBaseFieldInfo.setColumnType(deu, dateField, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.TEXT, 8);
      FieldSchema event0Field = new FieldSchema("event0","string", "TEXT");
      HBaseFieldInfo.setColumnType(deu, event0Field, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.WORD, 0);
      FieldSchema event1Field = new FieldSchema("event1","string", "TEXT");
      HBaseFieldInfo.setColumnType(deu, event1Field, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.WORD, 0);
      FieldSchema event2Field = new FieldSchema("event2","string", "TEXT");
      HBaseFieldInfo.setColumnType(deu, event2Field, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.WORD, 0);
      FieldSchema event3Field = new FieldSchema("event3","string", "TEXT");
      HBaseFieldInfo.setColumnType(deu, event3Field, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.WORD, 0);
      FieldSchema event4Field = new FieldSchema("event4","string", "TEXT");
      HBaseFieldInfo.setColumnType(deu, event4Field, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.WORD, 0);
      FieldSchema event5Field = new FieldSchema("event5","string", "TEXT");
      HBaseFieldInfo.setColumnType(deu, event5Field, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.WORD, 0);
      FieldSchema uidSampleHashField = new FieldSchema("uhash", "tinyint", "BINARY:1");
      HBaseFieldInfo.setColumnType(deu, uidSampleHashField, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.BINARY, 1);
      FieldSchema uidField = new FieldSchema("uid", "int", "BINARY:4");
      HBaseFieldInfo.setColumnType(deu, uidField, HBaseFieldInfo.FieldType.rowkey,
              null, null, HBaseFieldInfo.DataSerType.BINARY, 4);
      FieldSchema valueField = new FieldSchema("value","bigint", "BINARY:8");
      HBaseFieldInfo.setColumnType(deu, valueField, HBaseFieldInfo.FieldType.cellvalue,
              "val", "val", HBaseFieldInfo.DataSerType.BINARY, 8);
      FieldSchema timestampField = new FieldSchema("timestamp","bigint", "BINARY:8");
      HBaseFieldInfo.setColumnType(deu, timestampField, HBaseFieldInfo.FieldType.cversion,
              "val", "val", HBaseFieldInfo.DataSerType.BINARY, 8);
      deu.getSd().addToCols(dateField);
      deu.getSd().addToCols(event0Field);
      deu.getSd().addToCols(event1Field);
      deu.getSd().addToCols(event2Field);
      deu.getSd().addToCols(event3Field);
      deu.getSd().addToCols(event4Field);
      deu.getSd().addToCols(event5Field);
      deu.getSd().addToCols(uidSampleHashField);
      deu.getSd().addToCols(uidField);
      deu.getSd().addToCols(valueField);
      deu.getSd().addToCols(timestampField);

      TableInfo.setPrimaryKeyPattern(deu,
              "${date}${event0}.[${event1}.[${event2}.[${event3}.[${event4}.[${event5}]]]]]\\xFF${uhash}${uid}");
      client.createTable(deu);

  }

  private void createUserTable(String table) throws TException {
      HiveConf conf = new HiveConf();
      DefaultDrillHiveMetaClient client = new DefaultDrillHiveMetaClient(conf);
      String dbName="test_xa";
      Table user = TableInfo.newTable();
      user.setDbName(dbName);
      user.setTableName(table);

      client.dropTable(dbName, table);
      FieldSchema uidField = new FieldSchema("uid", "int", "BINARY:4");
      HBaseFieldInfo.setColumnType(user, uidField, HBaseFieldInfo.FieldType.rowkey, null, null, HBaseFieldInfo.DataSerType.BINARY, 4);
      FieldSchema ref0Field = new FieldSchema("ref0", "string", "TEXT");
      HBaseFieldInfo.setColumnType(user, ref0Field, HBaseFieldInfo.FieldType.cellvalue, "value", "ref0", HBaseFieldInfo.DataSerType.TEXT, 0);
      FieldSchema regTimeField = new FieldSchema("first_login_time", "bigint", "TEXT:14");
      HBaseFieldInfo.setColumnType(user, regTimeField, HBaseFieldInfo.FieldType.cellvalue, "value", "first_login_time", HBaseFieldInfo.DataSerType.TEXT, 14);
      user.getSd().addToCols(uidField);
      user.getSd().addToCols(ref0Field);
      user.getSd().addToCols(regTimeField);

      TableInfo.setPrimaryKeyPattern(user, "${uid}");
      client.createTable(user);
  }

  private void createUserIndex(String userIndexName) throws TException {
      HiveConf conf = new HiveConf();
      DefaultDrillHiveMetaClient client = new DefaultDrillHiveMetaClient(conf);
      String dbName="test_xa";
      Table userIndex = TableInfo.newTable();
      userIndex.setDbName(dbName);
      userIndex.setTableName(userIndexName);
      client.dropTable(dbName, userIndexName);
      FieldSchema propNumber = new FieldSchema("propnumber", Constants.SMALLINT_TYPE_NAME,"BINARY:2");
      HBaseFieldInfo.setColumnType(userIndex, propNumber, HBaseFieldInfo.FieldType.rowkey,
                                   null, null, HBaseFieldInfo.DataSerType.BINARY, 2);
      FieldSchema dateField = new FieldSchema("date", "int", "TEXT:8");
      HBaseFieldInfo.setColumnType(userIndex, dateField, HBaseFieldInfo.FieldType.rowkey,
                                    null, null, HBaseFieldInfo.DataSerType.TEXT, 8);
      FieldSchema uidField = new FieldSchema("uid", "int", "BINARY:4");
      HBaseFieldInfo.setColumnType(userIndex, uidField, HBaseFieldInfo.FieldType.cqname,
                                   "value", null, HBaseFieldInfo.DataSerType.BINARY, 4);
      userIndex.getSd().addToCols(propNumber);
      userIndex.getSd().addToCols(dateField);
      userIndex.getSd().addToCols(uidField);
      TableInfo.setPrimaryKeyPattern(userIndex, "${propnumber}${date}");
      client.createTable(userIndex);
  }



}
