package org.apache.drill.exec.engine.async;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

class RecordFrame {
  BatchSchema schema;
  SelectionVector4 sv4;
  SelectionVector2 sv2;
  List<ValueVector> vectors;
  int recordCount;
  FragmentContext context;
  
  RecordBatch.IterOutcome outcome;
}
