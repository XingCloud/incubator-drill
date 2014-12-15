package org.apache.drill.exec.engine.async;

import org.apache.drill.exec.rpc.NamedThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DriverPoolExecutor {
  private static DriverPoolExecutor instance = new DriverPoolExecutor();

  private ThreadPoolExecutor executor;

  public DriverPoolExecutor() {
    executor = new ThreadPoolExecutor(40, 40, 60000, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1024),
      new NamedThreadFactory("Drivers"));
  }

  public static DriverPoolExecutor getInstance() {
    return instance;
  }
  
  public void startDriver(LeafDriver driver){
    executor.submit(driver);
  }
}
