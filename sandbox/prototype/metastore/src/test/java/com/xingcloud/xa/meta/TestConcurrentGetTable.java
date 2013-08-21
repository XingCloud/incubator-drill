package com.xingcloud.xa.meta;

import com.xingcloud.meta.DefaultDrillHiveMetaClient;
import com.xingcloud.meta.MetaClientPool;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * User: Z J Wu Date: 13-8-21 Time: 下午5:22 Package: com.xingcloud.xa.meta
 */
public class TestConcurrentGetTable {

  @Test
  public void testGetTable() throws TException, InterruptedException, ExecutionException {
    int times = 20;
    final String table = "age_deu";
    Callable<Table> c = new Callable<Table>() {
      @Override
      public Table call() throws Exception {
        DefaultDrillHiveMetaClient client = null;
        boolean shouldDestroy = false;
        try{
          client = MetaClientPool.getInstance().pickClient();
          return client.getTable("test_xa", table);
        }catch(Exception e){
          e.printStackTrace();
          shouldDestroy = true;
        }finally{
          if(client != null){
            MetaClientPool.getInstance().returnClient(client, shouldDestroy);
          }
        }
        return null;
//        DefaultDrillHiveMetaClient2 client2 = DefaultDrillHiveMetaClient2.getInstance();
//        return client2.getTable("test_xa", table);
      }
    };

    ExecutorService es = Executors.newFixedThreadPool(3);
    List<Future<Table>> futureList = new ArrayList<>(times);
    Future<Table> future;
    for (int i = 0; i < times; i++) {
      future = es.submit(c);
      futureList.add(future);
    }

    for (Future<Table> f : futureList) {
      System.out.println(f.get());
    }

  }
  
  @Test
  public void testGetPooledTable() throws TException, InterruptedException, ExecutionException {
    int times = 20;
    final String table = "age_deu";
    Callable<Table> c = new Callable<Table>() {
      @Override
      public Table call() throws Exception {
        return MetaClientPool.getInstance().getPooledTable("test_xa", "age_deu");
      }
    };

    ExecutorService es = Executors.newFixedThreadPool(3);
    List<Future<Table>> futureList = new ArrayList<>(times);
    Future<Table> future;
    for (int i = 0; i < times; i++) {
      future = es.submit(c);
      futureList.add(future);
    }

    for (Future<Table> f : futureList) {
      System.out.println(f.get());
    }

  }
}
