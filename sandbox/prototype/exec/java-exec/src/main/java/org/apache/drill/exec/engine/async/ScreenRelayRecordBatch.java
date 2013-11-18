package org.apache.drill.exec.engine.async;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.slf4j.Logger;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/28/13
 * Time: 9:39 AM
 */

// for screen root  , call to next() will block until result is ready
public class ScreenRelayRecordBatch extends AbstractRelayRecordBatch {

  private final static Logger logger = org.slf4j.LoggerFactory.getLogger(ScreenRelayRecordBatch.class);


  private AsyncExecutor asyncExecutor;

  public ScreenRelayRecordBatch(AsyncExecutor asyncExecutor) {
    this.asyncExecutor = asyncExecutor;
    recordFrames = new LinkedBlockingDeque<>();
  }

  @Override
  public boolean nextStash() {
    return true;
  }

  @Override
  protected void stash(RecordFrame recordFrame) {
    recordFrames.add(recordFrame);
  }

  @Override
  public IterOutcome next() {
    if (!asyncExecutor.isStarted()) {
      asyncExecutor.start();
    }
    try {
      current = recordFrames.poll(3600, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      asyncExecutor.checkFinishStatus();
      logger.error("Query stopped .");
      throw new DrillRuntimeException("Time out");
    }
    if (current.outcome == IterOutcome.NONE) {
      asyncExecutor.checkFinishStatus();
      logger.info("Query finished .");
      asyncExecutor.worker.shutdown();
    }
    return current.outcome;
  }


  @Override
  public void kill() {
    asyncExecutor.submitKill();
    cleanUp();
    incoming.kill();
  }
}
