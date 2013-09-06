package org.apache.drill.exec.engine.async;


import org.apache.drill.exec.physical.impl.VectorHolder;
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
      return current.outcome;
    } catch (InterruptedException e) {
      e.printStackTrace();  
      return IterOutcome.STOP;      
    }
  }

  @Override
  public void kill() {
    super.kill();
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
  }

  @Override
  public void mirrorResultFromIncoming(IterOutcome incomingOutcome) {
    logger.debug("mirroring results...{}",incomingOutcome);
    RecordFrame frame = new RecordFrame();
    super.mirrorResultFromIncoming(incomingOutcome, incoming, frame);
    resultQueue.add(frame);
  }
}
