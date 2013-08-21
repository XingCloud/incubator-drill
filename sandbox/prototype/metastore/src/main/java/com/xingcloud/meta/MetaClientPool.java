package com.xingcloud.meta;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaClientPool {
  static final Logger logger = LoggerFactory.getLogger(MetaClientPool.class);
  private static MetaClientPool instance;

  private GenericObjectPool pool = null;


  public MetaClientPool() {
    pool = new GenericObjectPool(new ClientFactory(), 
      16, 
      GenericObjectPool.WHEN_EXHAUSTED_BLOCK, 
      GenericObjectPool.DEFAULT_MAX_WAIT);
  }

  public static MetaClientPool getInstance(){
    if(instance == null){
      instance = new MetaClientPool();
    }
    return instance;
  }
  
  public DefaultDrillHiveMetaClient pickClient(){
    try {
      return (DefaultDrillHiveMetaClient) pool.borrowObject();
    } catch (Exception e) {
      logger.warn("error borrowing objects", e);
      throw new IllegalStateException(e);
    }
  }
  
  public void returnClient(DefaultDrillHiveMetaClient client, boolean shouldDestroy){
    try {
      if(shouldDestroy){
        pool.invalidateObject(client);
      }else{
        pool.returnObject(client);
      }
    } catch (Exception e) {
      logger.warn("return object error", e);
    }
  }

  /**
   * helper method, for getting a table definition easily.
   * @param dbName dbName
   * @param tableName tableName
   * @return Table
   */
  public Table getPooledTable(String dbName,String tableName)  {
    DefaultDrillHiveMetaClient client = null;
    boolean shouldDestroy = false;
    try{
      client = MetaClientPool.getInstance().pickClient();
      return client.getTable(dbName, tableName);
    }catch(Exception e){
      e.printStackTrace();
      shouldDestroy = true;
    }finally{
      if(client != null){
        MetaClientPool.getInstance().returnClient(client, shouldDestroy);
      }
    }
    return null;
  }
  
  
  public void returnClient(DefaultDrillHiveMetaClient client){
    returnClient(client, false);
  }
  
  private class ClientFactory implements PoolableObjectFactory {
    @Override
    public Object makeObject() throws Exception {
      return new DefaultDrillHiveMetaClient(new HiveConf());
    }

    @Override
    public void destroyObject(Object obj) throws Exception {
      try{
        ((DefaultDrillHiveMetaClient)obj).close();
      }catch (Exception e){
        logger.warn("client destroy exception", e);
      }
    }

    @Override
    public boolean validateObject(Object obj) {
      return true;
    }

    @Override
    public void activateObject(Object obj) throws Exception {
    }

    @Override
    public void passivateObject(Object obj) throws Exception {
    }
  }
}
