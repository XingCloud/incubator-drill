package org.apache.drill.exec.engine.async;

import com.google.common.collect.Lists;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncExecutor {

//  Lock lock = new ReentrantLock(true);
  /**
   * mapping from POP to implementing RecordBatch
   */
  private Map<PhysicalOperator, RecordBatch> pop2OriginalBatch = new HashMap<>();

  private Map<RecordBatch, List<RelayRecordBatch>> batch2DirectParents = new HashMap<>();

  private Map<RecordBatch, List<RelayRecordBatch>> batch2DirectChildren = new HashMap<>();

  private boolean started = false;

  private List<LeafDriver> drivers = new ArrayList<>();

  private CountDownLatch driversStopped = null;

  private ThreadPoolExecutor executor = new ThreadPoolExecutor(24, 24, 10, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(1024), new NamedThreadFactory("Executor-"));

  static private Logger logger = LoggerFactory.getLogger(AsyncExecutor.class);

  /**
   * *************
   * data preparation
   * *************
   */


  public <T extends PhysicalOperator> RootExec createRootBatchWithChildren(RootCreator<T> creator, T operator, FragmentContext context, AsyncImplCreator asyncImplCreator) throws ExecutionSetupException {
    PhysicalOperator child = operator.iterator().next();
    RecordBatch childBatch = child.accept(asyncImplCreator, context);
    BlockingRelayRecordBatch relay = new BlockingRelayRecordBatch(this);
    getParentRelaysFor(childBatch).add(relay);
    relay.setIncoming(childBatch);
    relay.setParent(null);//no parent batch; only parent RootExec
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
        childRelays.add((SingleRelayRecordBatch) child);
        ((SingleRelayRecordBatch) child).setParent(batch);
      }
    }
    return batch;
  }

  private RecordBatch createOutputRelay(RecordBatch incoming) {
    SingleRelayRecordBatch relay = new SingleRelayRecordBatch();
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
    executor.shutdown();
    try {
      driversStopped.await();
      executor.awaitTermination(10, TimeUnit.MINUTES);
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
            logger.info("{} , {}", this.getClass(), o);
            upward(split, o);
            if (o == IterOutcome.NONE)
              break loopNext;
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
          if (o == IterOutcome.NONE)
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

  public void upward(RecordBatch recordBatch, IterOutcome o) {
    try {
      List<RelayRecordBatch> parents = getParentRelaysFor(recordBatch);
      for (RelayRecordBatch parent : parents) {
        parent.mirrorAndStash(o);
      }
      for (RelayRecordBatch parent : parents) {
        if (parent instanceof SingleRelayRecordBatch) {
          addTask(((SingleRelayRecordBatch) parent).parent);
        } else {
          logger.info("Output to BlockRelayRecordBatch .");
        }
      }
      if (o == IterOutcome.OK_NEW_SCHEMA || o == IterOutcome.OK) {
        for (ValueVector v : recordBatch) {
          v.clear();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void addTask(RecordBatch recordBatch) {
    Task task = new Task(recordBatch);
    executor.submit(task);
  }

  class Task implements Runnable {
    RecordBatch recordBatch;

    Task(RecordBatch recordBatch) {
      this.recordBatch = recordBatch;
    }

    @Override
    public void run() {
      synchronized (recordBatch) {
        while (true) {
          IterOutcome o;
          try {
            o = recordBatch.next();
          } catch (Exception e) {
            e.printStackTrace();
            upward(recordBatch, IterOutcome.STOP);
            return;
          }
          switch (o) {
            case OK_NEW_SCHEMA:
            case OK:
            case NONE:
              logger.info("{} upward {}", recordBatch.getClass(), o);
              upward(recordBatch, o);
              if (o == IterOutcome.NONE) {
                return;
              }
              break;
            case NOT_YET:
              logger.info("{} end with NOT_YET .", recordBatch.getClass().getName());
              return;
            case STOP:
              submitKill();
              return;
          }
        }
      }
    }
  }

}
