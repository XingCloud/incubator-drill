package com.xingcloud.meta;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.thrift.TException;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 8/6/13
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class CreateHBaseMeta {
    public static void createPropRegisterTable(String dbName,String tableName) throws TException {
        String[] propName={"coin_buy","coin_promotion","coin_initialstatus","grade","pay_amount",
                "first_pay_time","register_time","last_login_time","last_pay_time",
                "nation","geoip","identifier","platform","language","version",
                "ref","ref0","ref1","ref2","ref3","ref4"};
        DrillHiveMetaClient client = ProxyMetaClientFactory.getInstance().newProxiedPooledClient();
        Table regPropIndex = TableInfo.newTable();
        regPropIndex.setDbName(dbName);
        regPropIndex.setTableName(tableName);
        if(client.tableExists(dbName,tableName))
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

    public static void createDEUTable(String dbName,String table) throws TException {
        HiveConf conf = new HiveConf();
        DrillHiveMetaClient client = ProxyMetaClientFactory.getInstance().newProxiedPooledClient();
        Table deu = TableInfo.newTable();
        deu.setTableName(table);
        deu.setDbName(dbName);
        if(client.tableExists(dbName,table))
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
        FieldSchema timestampField = new FieldSchema("timestamp","int", "BINARY:4");
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

    public static void createUserTable(String dbName,String table) throws TException {
        DrillHiveMetaClient client = ProxyMetaClientFactory.getInstance().newProxiedPooledClient();
        Table user = TableInfo.newTable();
        user.setDbName(dbName);
        user.setTableName(table);
        if(client.tableExists(dbName,table))
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

    public static void createUserIndex(String dbName,String userIndexName) throws TException {
        DrillHiveMetaClient client = ProxyMetaClientFactory.getInstance().newProxiedPooledClient();
        Table userIndex = TableInfo.newTable();
        userIndex.setDbName(dbName);
        userIndex.setTableName(userIndexName);
        if(client.tableExists(dbName,userIndexName))
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
