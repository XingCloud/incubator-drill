package org.apache.drill.exec.engine.async;


import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * call to next() will block until result is ready
 */
public class BlockingRelayRecordBatch extends SingleRelayRecordBatch implements RelayRecordBatch{

  static final Logger logger = LoggerFactory.getLogger(BlockingRelayRecordBatch.class);
  private VectorHolder vh = null;    

  BlockingDeque<RecordFrame> resultQueue = new LinkedBlockingDeque<>();
  
  AsyncExecutor executor = null;
  
  RecordFrame current = null;

  public BlockingRelayRecordBatch(AsyncExecutor executor) {
    this.executor = executor;
  }

  @Override
  public IterOutcome next() {
    if(!executor.isStarted()){
      executor.start();
    }
    try {
      if(current!= null){
        cleanupVectors(current);
      }
      current = resultQueue.poll(Long.MAX_VALUE, TimeUnit.SECONDS);
      if(current.nextErrorCause != null){
        throw current.nextErrorCause;
      }
      IterOutcome ret = current.outcome;
      if(ret == null){
        logger.warn("current outcome null!", new NullPointerException());
      }
      return ret;
    } catch (InterruptedException e) {
      e.printStackTrace();  
      return IterOutcome.STOP;      
    }
  }

  @Override
  public void kill() {
    //clean up after driver shutdown
    //this method would block, until all drivers stopped, and all batches killed.
    executor.submitKill();
    this.postCleanup();
    incoming.kill();
  }

  @Override
  public void postCleanup() {
    super.postCleanup();
    while(!resultQueue.isEmpty()){
      cleanupVectors(resultQueue.poll());
    }
  }

  public RecordFrame getCurrent() {
    return current;
  }

  @Override
  public void markNextFailed(RuntimeException cause) {
    RecordFrame frame = new RecordFrame();    
    frame.nextErrorCause = cause;
    resultQueue.add(frame);
    if(cause == null){
      logger.warn("errorCause null!", new NullPointerException());
    }
  }

  @Override
  public void mirrorResultFromIncoming(IterOutcome incomingOutcome, boolean needTransfer) {
    logger.debug("mirroring results...{}",incomingOutcome);
    RecordFrame frame = new RecordFrame();
    super.mirrorResultFromIncoming(incomingOutcome, incoming, frame, needTransfer);
    resultQueue.add(frame);
    if(incomingOutcome == null){
      logger.warn("incomingOutcome null!", new NullPointerException());      
    }
  }
}
