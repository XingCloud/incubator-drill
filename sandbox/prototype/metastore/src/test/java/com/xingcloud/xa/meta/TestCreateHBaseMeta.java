package com.xingcloud.xa.meta;

import com.xingcloud.meta.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 8/6/13
 * Time: 11:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestCreateHBaseMeta {
    String pid="age";
    String hivePid=pid.replaceAll("-","Mns");
    String tableName = hivePid+"_deu";
    String dbName = "test_xa";
    String userTableName = "user_"+hivePid;
    String userIndexName = "property_"+hivePid+"_index";
    String userRegPropName="register_template_prop_index";
    @Test
    public void createEventMeta() throws TException {
        CreateHBaseMeta.createDEUTable(dbName, tableName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, tableName);
        printColumns(table);
    }
    @Test
    public void createUserMeta() throws TException{
        CreateHBaseMeta.createUserTable(dbName,userTableName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userTableName);
        printColumns(table);
    }
    @Test
    public void createUserIndexMeta() throws TException{
        CreateHBaseMeta.createUserIndex(dbName,userIndexName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userIndexName);
        printColumns(table);

    }
    @Test
    public void createPropMeta() throws TException{
        CreateHBaseMeta.createPropRegisterTable(dbName,userRegPropName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userRegPropName);
        printColumns(table);
    }
    @Test
    public void createMetas() throws TException{
        createEventMeta();
        createUserMeta();
        createUserIndexMeta();
        createPropMeta();
    }
    private void printColumns(Table table) {
        List<KeyPart> pkSequence = TableInfo.getPrimaryKey(table);
        System.out.println("pkSequence = " + pkSequence);
        for(FieldSchema fieldSchema: table.getSd().getCols()){
            HBaseFieldInfo fieldInfo = HBaseFieldInfo.getColumnType(table, fieldSchema);
            System.out.println("fieldInfo = " + fieldInfo);
        }
    }
}
