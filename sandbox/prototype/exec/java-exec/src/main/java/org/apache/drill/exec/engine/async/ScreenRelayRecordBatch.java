package org.apache.drill.exec.engine.async;

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
      current = recordFrames.poll(60000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      return IterOutcome.STOP;
    }
    if (current.outcome == IterOutcome.NONE) {
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
