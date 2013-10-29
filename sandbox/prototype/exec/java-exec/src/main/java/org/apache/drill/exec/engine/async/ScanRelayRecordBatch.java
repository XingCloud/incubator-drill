package org.apache.drill.exec.engine.async;

import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/28/13
 * Time: 9:39 AM
 */

// for split scan & scan   , call to stash() will block until there is enough space
public class ScanRelayRecordBatch extends AbstractRelayRecordBatch {
  private final static int DEFAULT_CAPACITY = 1;

  private int capacity;

  public ScanRelayRecordBatch() {
    init(DEFAULT_CAPACITY);
  }

  public ScanRelayRecordBatch(int capacity) {
    init(capacity);
  }

  private void init(int capacity) {
    recordFrames = new ArrayBlockingQueue<>(capacity);
  }

  @Override
  protected void stash(RecordFrame recordFrame) {
    try {
      recordFrames.offer(recordFrame, 60000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new DrillRuntimeException("Stash timeout ");
    }
  }

  @Override
  public IterOutcome next() {
    current = recordFrames.poll();
    if (current == null) {
      return IterOutcome.NOT_YET;
    }
    return current.outcome;

  }

  @Override
  public void kill() {
    cleanUp();
    incoming.kill();
  }
}
