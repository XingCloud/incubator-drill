package com.xingcloud.meta;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.lang.reflect.*;

public class ProxyMetaClientFactory {

  public static final ProxyMetaClientFactory instance = new ProxyMetaClientFactory();

  private final Class<?> proxyClass;
  private final Constructor<?> constructor;

  public ProxyMetaClientFactory() {
    try{
      proxyClass = Proxy.getProxyClass(IMetaStoreClient.class.getClassLoader(),
        new Class[]{IMetaStoreClient.class});
      constructor = proxyClass.getConstructor(new Class[]{InvocationHandler.class});
    }catch (Exception e){
      throw new IllegalArgumentException("error initializing proxy!", e); 
    }
  }

  public static ProxyMetaClientFactory getInstance() {
    return instance;
  }

  public IMetaStoreClient newProxiedPooledClient(){
    try {
      return (IMetaStoreClient) constructor.newInstance(new MHandler());
    } catch (Exception e) {
      throw new IllegalArgumentException("creating instance error!", e);
    }
  }
  
  static class MHandler implements InvocationHandler{

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      DefaultDrillHiveMetaClient client = null;
      boolean shouldDestroy = false;
      try{
        client = MetaClientPool.getInstance().pickClient();
        return method.invoke(client, args);
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
  }
}
