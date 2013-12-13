package org.apache.drill.exec.engine.async;

import org.slf4j.Logger;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/28/13
 * Time: 9:39 AM
 */

// for common physical operator except scan & screen
public class SimpleRelayRecordBatch extends AbstractRelayRecordBatch {

  // for test
  private boolean finished = false ;
  private AsyncExecutor asyncExecutor ;
  private final static Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleRelayRecordBatch.class);

  public SimpleRelayRecordBatch(AsyncExecutor asyncExecutor) {
    this.asyncExecutor = asyncExecutor ;
    recordFrames = new LinkedBlockingDeque<>();
  }

  @Override
  protected void stash(RecordFrame recordFrame) {
    if(finished){
      if(recordFrame.outcome != IterOutcome.NONE){
         logger.error("{} after NONE in {}.",recordFrame.outcome,incoming);
      }
      return ;
    }
    recordFrames.add(recordFrame);
  }

  @Override
  public IterOutcome next() {
    if(finished){
      RecordFrame recordFrame ;
      while((recordFrame = recordFrames.poll()) != null){
        if(recordFrame.outcome != IterOutcome.NONE){
          logger.error("{} after NONE in {}",recordFrame.outcome,incoming);
        }
      }
      return IterOutcome.NONE;
    }
    current = recordFrames.poll();
    if (current == null) {
      return IterOutcome.NOT_YET;
    }
    if(current.outcome == IterOutcome.NONE){
      asyncExecutor.markRecordBatchFinish(incoming);
      finished = true ;
    }
    return current.outcome;
  }


  @Override
  public void kill() {
    cleanUp();
    incoming.kill();
  }
}
