package org.apache.drill.exec.engine.async;

import org.apache.drill.exec.record.RecordBatch;

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
