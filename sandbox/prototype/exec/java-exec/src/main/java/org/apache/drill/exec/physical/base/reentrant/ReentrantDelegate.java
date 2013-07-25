package org.apache.drill.exec.physical.base.reentrant;

import org.apache.drill.exec.record.buffered.IterationBuffer;

public class ReentrantDelegate implements ReentrantOperator {
  
  private IterationBuffer buffer = null;
  
  @Override
  public IterationBuffer getIterationBuffer() {
    return this.buffer;
  }

  @Override
  public void initIterationBuffer(IterationBuffer buffer) {
    this.buffer = buffer;
  }
}
