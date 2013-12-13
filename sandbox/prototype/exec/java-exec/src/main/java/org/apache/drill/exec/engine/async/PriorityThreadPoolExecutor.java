package org.apache.drill.exec.engine.async;

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/29/13
 * Time: 6:37 PM
 */
public class PriorityThreadPoolExecutor extends ThreadPoolExecutor {

  public PriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return new ComparableFutureTask<>(runnable, value);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new ComparableFutureTask<>(callable);
  }

  protected class ComparableFutureTask<V> extends FutureTask<V> implements Comparable<ComparableFutureTask<V>>{

    private  Object object;
    public ComparableFutureTask(Callable<V> callable) {
      super(callable);
      object = callable ;
    }

    public ComparableFutureTask(Runnable runnable, V result) {
      super(runnable, result);
      object = runnable;
    }

    @Override
    public int compareTo(ComparableFutureTask<V> o) {
      if(o == null)
        return -1 ;
      return ((Comparable)object).compareTo(o.object);
    }
  }
}
