package org.apache.drill.exec.engine.async;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.RootCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.rpc.NamedThreadFactory;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class AsyncExecutor {

//  Lock lock = new ReentrantLock(true);
  /**
   * mapping from POP to implementing RecordBatch
   */
  private Map<PhysicalOperator, RecordBatch> pop2OriginalBatch = new HashMap<>();

  private Map<RecordBatch, List<RelayRecordBatch>> batch2DirectParents = new HashMap<>();

  private Map<RecordBatch, List<RelayRecordBatch>> batch2DirectChildren = new HashMap<>();

  private Map<RecordBatch, Integer> batchPriority = new HashMap<>();

  // for debug , record record batches which are not finished .
  private Map<RecordBatch,Object> recordBatches = Maps.newConcurrentMap();

  private boolean started = false;

  private List<LeafDriver> drivers = new ArrayList<>();

  private CountDownLatch driversStopped = null;

  public ThreadPoolExecutor worker = new PriorityThreadPoolExecutor(16, 16, 60, TimeUnit.MINUTES, new PriorityBlockingQueue<Runnable>(), new NamedThreadFactory("Worker-"));


  static private Logger logger = LoggerFactory.getLogger(AsyncExecutor.class);

  /**
   * *************
   * data preparation
   * *************
   */


  public <T extends PhysicalOperator> RootExec createRootBatchWithChildren(RootCreator<T> creator, T operator, FragmentContext context, AsyncImplCreator asyncImplCreator) throws ExecutionSetupException {
    PhysicalOperator child = operator.iterator().next();
    RecordBatch childBatch = child.accept(asyncImplCreator, context);
    ScreenRelayRecordBatch relay = new ScreenRelayRecordBatch(this);
    getParentRelaysFor(childBatch).add(relay);
    relay.setIncoming(childBatch);
    relay.setParent(null);//no parent batch; only parent RootExec
    initPriority(childBatch);
    return creator.getRoot(context, operator, Arrays.asList((RecordBatch) relay));
  }

  /**
   * create a new RecordBatch corresponding to PhysicalOperator;
   *
   * @param creator
   * @param operator
   * @param context
   * @param asyncImplCreator
   * @param <T>
   * @return
   * @throws ExecutionSetupException
   */
  public <T extends PhysicalOperator> RecordBatch createBatchWithChildrenRelay(BatchCreator<T> creator, T operator, FragmentContext context, AsyncImplCreator asyncImplCreator) throws ExecutionSetupException {
    return createBatch(creator, operator, context, asyncImplCreator, true);
  }

  public <T extends PhysicalOperator> RecordBatch createBatchWithoutChildrenRelay(BatchCreator<T> creator, T operator, FragmentContext context, AsyncImplCreator asyncImplCreator) throws ExecutionSetupException {
    return createBatch(creator, operator, context, asyncImplCreator, false);
  }

  public <T extends PhysicalOperator> RecordBatch createBatch(BatchCreator<T> creator, T operator, FragmentContext context, AsyncImplCreator asyncImplCreator, boolean relayChildren) throws ExecutionSetupException {
    if (pop2OriginalBatch.get(operator) != null) {
      return pop2OriginalBatch.get(operator);
    }
    RecordBatch batch = null;
    List<RecordBatch> children = Lists.newArrayList();
    for (PhysicalOperator child : operator) {
      if (relayChildren) {
        children.add(createOutputRelay(child.accept(asyncImplCreator, context)));
      } else {
        children.add(child.accept(asyncImplCreator, context));
      }
    }
    batch = creator.getBatch(context, operator, children);
    pop2OriginalBatch.put(operator, batch);
    List<RelayRecordBatch> childRelays = getChildRelaysFor(batch);
    if (relayChildren) {
      for (int i = 0; i < children.size(); i++) {
        RecordBatch child = children.get(i);
        childRelays.add((AbstractRelayRecordBatch) child);
        ((AbstractRelayRecordBatch) child).setParent(batch);
      }
    }
    return batch;
  }

  private RecordBatch createOutputRelay(RecordBatch incoming) {
    AbstractRelayRecordBatch relay = null;
    if (incoming instanceof UnionedScanBatch.UnionedScanSplitBatch || incoming instanceof ScanBatch) {
      relay = new ScanRelayRecordBatch();
    } else {
      relay = new SimpleRelayRecordBatch(this);
      recordBatches.put(incoming,new Object());
    }
    relay.setIncoming(incoming);
    List<RelayRecordBatch> relays = getParentRelaysFor(incoming);
    relays.add(relay);
    return relay;
  }

  private List<RelayRecordBatch> getChildRelaysFor(RecordBatch incoming) {
    List<RelayRecordBatch> ret = batch2DirectChildren.get(incoming);
    if (ret == null) {
      ret = new ArrayList<>();
      batch2DirectChildren.put(incoming, ret);
    }
    return ret;
  }

  private List<RelayRecordBatch> getParentRelaysFor(RecordBatch incoming) {
    List<RelayRecordBatch> ret = batch2DirectParents.get(incoming);
    if (ret == null) {
      ret = new ArrayList<>();
      batch2DirectParents.put(incoming, ret);
    }
    return ret;
  }

  /**
   * *************
   * execution
   * *************
   */

  public boolean isStarted() {
    return this.started;
  }

  public void start() {
    this.started = true;
    startDrivers();
  }

  // for debug
  public void recordFinish(RecordBatch recordBatch){
    recordBatches.remove(recordBatch);
  }

  public void checkStatus(){
    for(RecordBatch recordBatch : recordBatches.keySet()){
      logger.error("{} not finised . ",recordBatch);
    }
  }

  private void startDrivers() {
    //search for leaf nodes, and start driver for each
    Set<RecordBatch> leaves = new HashSet<>();
    Map<UnionedScanBatch, List<UnionedScanBatch.UnionedScanSplitBatch>> unioned2Splits = new HashMap<>();
    for (Map.Entry<PhysicalOperator, RecordBatch> entry : pop2OriginalBatch.entrySet()) {
      PhysicalOperator pop = entry.getKey();
      RecordBatch batch = entry.getValue();
      if (getChildRelaysFor(batch).size() == 0) {
        if (batch instanceof UnionedScanBatch.UnionedScanSplitBatch) {
          UnionedScanBatch incoming = ((UnionedScanBatch.UnionedScanSplitBatch) batch).getIncoming();
          List<UnionedScanBatch.UnionedScanSplitBatch> splits = unioned2Splits.get(incoming);
          if (splits == null) {
            splits = new ArrayList<>();
            unioned2Splits.put(incoming, splits);
          }
          splits.add((UnionedScanBatch.UnionedScanSplitBatch) batch);
        } else {
          //collect
          leaves.add(batch);
        }
      }
    }

    //start drivers
    driversStopped = new CountDownLatch(leaves.size());
    for (RecordBatch batch : leaves) {
      LeafDriver driver = null;
      if (batch instanceof UnionedScanBatch) {
        driver = new UnionedScanDriver((UnionedScanBatch) batch, unioned2Splits.get(batch));
      } else if (batch instanceof ScanBatch) {
        driver = new ScanDriver((ScanBatch) batch);
      }
      drivers.add(driver);
      DriverPoolExecutor.getInstance().startDriver(driver);
    }
  }

  public void submitKill() {
    for (int i = 0; i < drivers.size(); i++) {
      LeafDriver driver = drivers.get(i);
      driver.stop();
    }
    worker.shutdown();
    try {
      driversStopped.await();
      worker.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public class UnionedScanDriver implements LeafDriver {
    UnionedScanBatch unionedScanBatch;
    List<UnionedScanBatch.UnionedScanSplitBatch> splits;
    private boolean stopped = false;

    public UnionedScanDriver(UnionedScanBatch unionedScanBatch, List<UnionedScanBatch.UnionedScanSplitBatch> splits) {
      this.unionedScanBatch = unionedScanBatch;
      UnionedScanBatch.UnionedScanSplitBatch[] splitArray = splits.toArray(new UnionedScanBatch.UnionedScanSplitBatch[splits.size()]);
      this.splits = splits;
      //sort split according to entry order
      Arrays.sort(splitArray, new SplitComparator(unionedScanBatch));
      this.splits = Arrays.asList(splitArray);
    }

    @Override
    public void run() {
      try {
        logger.info("UnionedScanBatch driver[{}] start", unionedScanBatch);
        loopSplit:
        for (int i = 0; i < splits.size(); i++) {
          UnionedScanBatch.UnionedScanSplitBatch split = splits.get(i);
          loopNext:
          while (!stopped) {
            RecordBatch.IterOutcome o = split.next();
            upward(split, o);
            if (o == IterOutcome.NONE)
              break loopNext;
            if(o == IterOutcome.STOP)
              break loopSplit;
          }
        }
        logger.info("UnionedScanBatch driver[{}] exit", unionedScanBatch);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        driverStopped(this);
      }
    }

    @Override
    public void stop() {
      this.stopped = true;
    }
  }

  public static class SplitComparator implements Comparator<UnionedScanBatch.UnionedScanSplitBatch> {


    private final UnionedScanBatch usb;

    public SplitComparator(UnionedScanBatch unionedScanBatch) {
      this.usb = unionedScanBatch;
    }

    @Override
    public int compare(UnionedScanBatch.UnionedScanSplitBatch o1, UnionedScanBatch.UnionedScanSplitBatch o2) {
      int[] table = usb.getOriginalID2sorted();
      return table[o1.getPop().getEntries()[0]] - table[o2.getPop().getEntries()[0]];
    }
  }

  public class ScanDriver implements LeafDriver {
    ScanBatch scanBatch;
    private boolean stopped = false;

    public ScanDriver(ScanBatch scanBatch) {
      this.scanBatch = scanBatch;
    }

    @Override
    public void run() {
      try {
        logger.info("ScanBatch driver[{}] start .", scanBatch);
        while (!stopped) {
          IterOutcome o = scanBatch.next();
          upward(scanBatch, o);
          if (o == IterOutcome.NONE || o == IterOutcome.STOP)
            break;
        }
        logger.info("ScanBatch driver[{}] exit .", scanBatch);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        driverStopped(this);
      }
    }

    @Override
    public void stop() {
      this.stopped = true;
    }
  }

  private void driverStopped(LeafDriver driver) {
    driversStopped.countDown();
    logger.info("Running drivers : {} ", driversStopped.getCount());
  }

  public boolean upward(RecordBatch recordBatch, IterOutcome o) {
    boolean nextStash = true;
    try {
      List<RelayRecordBatch> parents = getParentRelaysFor(recordBatch);
      for (RelayRecordBatch parent : parents) {
        parent.mirrorAndStash(o);
        nextStash &= parent.nextStash();
      }
      if (o == IterOutcome.OK_NEW_SCHEMA || o == IterOutcome.OK) {
        for (ValueVector v : recordBatch) {
          v.clear();
        }
      }
      for (RelayRecordBatch parent : parents) {
        if (parent instanceof ScreenRelayRecordBatch) {
          // do nothing
        } else {
          addTask(((AbstractRelayRecordBatch) parent).parent);
        }
      }
    } catch (Exception e) {
      logger.error("{} upward {} failed .", recordBatch, o);
      throw e;
    }
    return nextStash;
  }

  public void addTask(RecordBatch recordBatch) {
    Task task = new Task(recordBatch);
    try {
      worker.submit(task);
    } catch (Exception e) {
      if (!worker.isTerminating() && !worker.isShutdown())
        throw new DrillRuntimeException("Submit task failed .");
    }
  }

  class Task implements Runnable, Comparable<Task> {
    RecordBatch recordBatch;

    Task(RecordBatch recordBatch) {
      this.recordBatch = recordBatch;
    }

    @Override
    public int compareTo(Task o) {
      return getPriority(recordBatch).compareTo(getPriority(o.recordBatch));
    }

    @Override
    public void run() {
      synchronized (recordBatch) {
        while (true) {
          try {
            IterOutcome o = recordBatch.next();
            switch (o) {
              case OK_NEW_SCHEMA:
              case OK:
              case NONE:
                boolean next = upward(recordBatch, o);
                if (o == IterOutcome.NONE) {
                  return;
                }
                if (!next) {
                  addTask(recordBatch);
                  return;
                }
                break;
              case NOT_YET:
                return;
              case STOP:
                upward(recordBatch, IterOutcome.STOP);
                return;
            }
          } catch (Exception e) {
            e.printStackTrace();
            recordBatch.getContext().fail(e);
            upward(recordBatch, IterOutcome.STOP);
            return;
          }
        }
      }

    }

  }

  public void initPriority(RecordBatch recordBatch) {
    Integer priority = batchPriority.get(recordBatch);
    if (priority == null) {
      priority = 0;
      batchPriority.put(recordBatch, priority);
    }
    List<RelayRecordBatch> incomingRelays = batch2DirectChildren.get(recordBatch);
    if (incomingRelays != null) {
      int incomingPriority = priority + 1;
      for (RelayRecordBatch relay : incomingRelays) {
        RecordBatch incoming = relay.getIncoming();
        batchPriority.put(incoming, incomingPriority);
        initPriority(incoming);
      }
    }
  }

  public Integer getPriority(RecordBatch recordBatch) {
    return batchPriority.get(recordBatch);
  }

}
