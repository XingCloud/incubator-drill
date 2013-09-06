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
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AsyncExecutor {

  /**
   * mapping from POP to implementing RecordBatch
   */
  private Map<PhysicalOperator, RecordBatch> pop2OriginalBatch = new HashMap<>();

  private Map<RecordBatch, List<RelayRecordBatch>> batch2DirectParents = new HashMap<>();

  private Map<RecordBatch, List<RelayRecordBatch>> batch2DirectChildren = new HashMap<>();

  private boolean started = false;

  private List<LeafDriver> drivers = new ArrayList<>();
  private Logger logger = LoggerFactory.getLogger(AsyncExecutor.class);

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
    if (relayChildren) {
      List<RelayRecordBatch> childRelays = getChildRelaysFor(batch);
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


  public class UnionedScanDriver implements LeafDriver {
    UnionedScanBatch unionedScanBatch;
    List<UnionedScanBatch.UnionedScanSplitBatch> splits;

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
      logger.debug("driver start");
      for (int i = 0; i < splits.size(); i++) {
        UnionedScanBatch.UnionedScanSplitBatch split = splits.get(i);
        loop:
        while (true) {
          RecordBatch.IterOutcome outcome = nextUpward(split);
          switch (outcome) {
            case NONE:
            case STOP:
              break loop;
          }
        }
      }
      logger.debug("driver exit");
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

    public ScanDriver(ScanBatch scanBatch) {
      this.scanBatch = scanBatch;
    }

    @Override
    public void run() {
      logger.debug("driver start");
      loop:
      while (true) {
        try {
          RecordBatch.IterOutcome outcome = nextUpward(scanBatch);
          switch (outcome) {
            case NONE:
            case STOP:
              break loop;
          }
        } catch (Exception e) {
          logger.warn("driver failed:", e);
        }

      }
      logger.debug("driver exit");

    }
  }

  private RecordBatch.IterOutcome nextUpward(RecordBatch batch) {
    RecordBatch.IterOutcome outcome = null;
    Throwable errorCause = null;
    while (true) {
      boolean parentKilled = false;
      try {
        outcome = batch.next();
      } catch (Throwable e) {
        errorCause = e;
      }
      if (outcome == RecordBatch.IterOutcome.NOT_YET) {
        //data not ready yet, no need to continue upward
        return outcome;
      }
      List<RelayRecordBatch> parents = getParentRelaysFor(batch);
      if(parents.size()==1 && parents.get(0) instanceof BlockingRelayRecordBatch){
        //it's Root up there. so transfer vectors early.
        BlockingRelayRecordBatch blockingRelay = (BlockingRelayRecordBatch) parents.get(0);
        if(blockingRelay.isKilled()){
          parentKilled = true;
        }else{
          if(errorCause != null){
            if (errorCause instanceof RuntimeException) {
              blockingRelay.markNextFailed((RuntimeException) errorCause);
            } else {
              blockingRelay.markNextFailed(new RuntimeException(errorCause));
            }
          }else{
            blockingRelay.mirrorResultFromIncoming(outcome, true); 
          }
        }
      }else{// not BlockingRelayRecordBatch
        for (int i = 0; i < parents.size(); i++) {
          RelayRecordBatch parentRelay = parents.get(i);
          if (parentRelay.isKilled()) {
            parentKilled = true;
            break;
          } else {
            if (errorCause != null) {
              if (errorCause instanceof RuntimeException) {
                parentRelay.markNextFailed((RuntimeException) errorCause);
              } else {
                parentRelay.markNextFailed(new RuntimeException(errorCause));
              }
            } else {
              parentRelay.mirrorResultFromIncoming(outcome, false);
            }
          }
        }
        if (outcome == RecordBatch.IterOutcome.OK_NEW_SCHEMA || outcome == RecordBatch.IterOutcome.OK) {
          for (ValueVector v : batch) {
            v.clear();
          }
        }
      }

      if (parentKilled) {
        return RecordBatch.IterOutcome.STOP;
      }
      for (int i = 0; i < parents.size(); i++) {
        RelayRecordBatch parentRelay = parents.get(i);
        if (parentRelay instanceof BlockingRelayRecordBatch) {
          //do nothing
        } else {
          nextUpward(((SingleRelayRecordBatch) parentRelay).parent);
        }
      }
      if (outcome == RecordBatch.IterOutcome.NONE || outcome == RecordBatch.IterOutcome.STOP || outcome == RecordBatch.IterOutcome.NOT_YET) {
        break;
      }
    }
    return outcome;
  }


}
