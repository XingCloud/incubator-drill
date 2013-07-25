package org.apache.drill.exec.physical.base.reentrant;

import org.apache.drill.exec.record.buffered.IterationBuffer;

public interface ReentrantOperator {
  IterationBuffer getIterationBuffer();
  void initIterationBuffer(IterationBuffer buffer);
}
