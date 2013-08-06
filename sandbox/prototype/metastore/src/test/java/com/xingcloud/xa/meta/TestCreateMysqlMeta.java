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
 * Time: 10:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestCreateMysqlMeta {
    String dbName = "test_xa";
    String pid="testtable_1000W";
    String hivePid=pid.replaceAll("-","Mns");
    //String tableName = hivePid+"_deu";
    //String userTableName = "user_"+hivePid;
    String userIndexName = "mysql_property_"+hivePid;
    String userRegPropName="mysql_register_template_prop";

    /*
    @Test
    public void createUserMeta() throws TException{
        CreateMysqlMeta.createUserTable(dbName,userTableName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userTableName);
        printColumns(table);
    }*/
    @Test
    public void createUserIndexMeta() throws TException{
        CreateMysqlMeta.createUserIndex(dbName,userIndexName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userIndexName);
        printColumns(table);

    }
    @Test
    public void createPropMeta() throws TException{
        CreateMysqlMeta.createPropRegisterTable(dbName,userRegPropName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userRegPropName);
        printColumns(table);
    }
    @Test
    public void createMetas() throws TException{
        //createUserMeta();
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
