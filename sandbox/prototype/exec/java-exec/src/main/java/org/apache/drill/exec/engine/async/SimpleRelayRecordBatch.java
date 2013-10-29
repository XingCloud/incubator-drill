package org.apache.drill.exec.engine.async;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/28/13
 * Time: 9:39 AM
 */

// for common physical operator except scan & screen
public class SimpleRelayRecordBatch extends AbstractRelayRecordBatch {

  public SimpleRelayRecordBatch() {
    recordFrames = new LinkedBlockingDeque<>();
  }

  @Override
  protected void stash(RecordFrame recordFrame) {
    recordFrames.add(recordFrame);
  }

  @Override
  public IterOutcome next() {
    synchronized (this) {
      current = recordFrames.poll();
      if (current == null) {
        changeState(State.WAITING);
        return IterOutcome.NOT_YET;
      }
      changeState(State.RUNNING);
      return current.outcome;
    }
  }

  @Override
  public boolean isSubmittable() {
    synchronized (this) {
      if (state == State.WAITING) {
        state = State.RUNNABLE;
        return true;
      }
      return false;
    }
  }

  @Override
  public void kill() {
    cleanUp();
    incoming.kill();
  }
}
