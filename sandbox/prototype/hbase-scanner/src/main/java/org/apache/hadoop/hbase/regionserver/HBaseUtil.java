package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-12-12
 * Time: 下午5:13
 */
public class HBaseUtil {

    public final static ExecutorService executor = new ThreadPoolExecutor(500,500,1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
}
