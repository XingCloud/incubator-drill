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

    @Test
    public void createMetas() throws TException {
        String pids[]={"ddt","v9-v9","onepiece","age","sof-dsk","sof-newgdp","web337","xiaomi"};
        for(int i=6;i<pids.length;i++){
            createMeta(pids[i]);
        }
    }

    public void createMeta(String pid) throws TException {
        String dbName = "test_xa";
        String hivePid=pid.replaceAll("-","Mns");
        //String tableName = hivePid+"_deu";
        //String userTableName = "user_"+hivePid;
        String userIndexName = "mysql_property_"+hivePid;
        String userRegPropName="mysql_register_template_prop";
        createUserIndexMeta(dbName,userIndexName);
        createPropMeta(dbName,userRegPropName);
    }


    /*
    @Test
    public void createUserMeta() throws TException{
        CreateMysqlMeta.createUserTable(dbName,userTableName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userTableName);
        printColumns(table);
    }*/
    public void createUserIndexMeta(String dbName,String userIndexName) throws TException{
        CreateMysqlMeta.createUserIndex(dbName,userIndexName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userIndexName);
        printColumns(table);

    }
    public void createPropMeta(String dbName,String userRegPropName) throws TException{
        CreateMysqlMeta.createPropRegisterTable(dbName,userRegPropName);
        DefaultDrillHiveMetaClient client=DefaultDrillHiveMetaClient.createClient();
        Table table = client.getTable(dbName, userRegPropName);
        printColumns(table);
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
