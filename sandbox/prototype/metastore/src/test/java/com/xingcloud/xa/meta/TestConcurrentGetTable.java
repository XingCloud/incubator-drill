package com.xingcloud.xa.meta;

import com.xingcloud.meta.DefaultDrillHiveMetaClient2;
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
    int times = 2;
    final String table = "age_deu";

    Callable<Table> c = new Callable<Table>() {
      @Override
      public Table call() throws Exception {
        DefaultDrillHiveMetaClient2 client2 = DefaultDrillHiveMetaClient2.getInstance();
        return client2.getTable("test_xa", table);
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
