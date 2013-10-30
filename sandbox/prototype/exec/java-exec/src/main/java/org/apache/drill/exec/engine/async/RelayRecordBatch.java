package org.apache.drill.exec.engine.async;

import org.apache.drill.exec.record.RecordBatch;

public interface RelayRecordBatch extends RecordBatch {
  void mirrorAndStash(IterOutcome o);
  void cleanUp();
  RecordBatch getIncoming();
  RecordBatch getParent();
  boolean nextStash();
}
