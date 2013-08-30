package org.apache.drill.exec.record.buffered;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

public interface BufferedStackFrame {
  BatchSchema getSchema();

  int getRecordCount();

  SelectionVector2 getSelectionVector2();

  SelectionVector4 getSelectionVector4();

  List<VectorWrapper<? extends ValueVector>> getVectors();

  RecordBatch.IterOutcome getOutcome();
  
  FragmentContext getContext();
}
