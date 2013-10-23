package org.apache.drill.exec.engine.async;

import org.apache.drill.exec.record.RecordBatch;

public interface RelayRecordBatch extends RecordBatch {
  void markNextFailed(RuntimeException cause);
  void mirrorAndStash(IterOutcome o);
  void postCleanup();
  boolean isKilled();
}
