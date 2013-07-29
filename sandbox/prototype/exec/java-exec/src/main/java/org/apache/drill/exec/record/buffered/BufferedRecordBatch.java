package org.apache.drill.exec.record.buffered;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Iterator;

public class BufferedRecordBatch implements RecordBatch {


  private final IterationBuffer iterationBuffer;

  BufferedStackFrame current;
  
  private VectorHolder vh;
  
  public BufferedRecordBatch(IterationBuffer iterationBuffer) {
    this.iterationBuffer = iterationBuffer;
    iterationBuffer.init(this);
  }

  @Override
  public FragmentContext getContext() {
    return current.getContext();
  }

  @Override
  public BatchSchema getSchema() {
    return current.getSchema();
  }

  @Override
  public int getRecordCount() {
    return current.getRecordCount();
  }

  @Override
  public void kill() {
    iterationBuffer.abort(this);
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return current.getSelectionVector2();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return current.getSelectionVector4();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return vh.getValueVector(path);
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    return vh.getValueVector(fieldId, clazz);
  }

  @Override
  public IterOutcome next() {
    if(!iterationBuffer.forward(this)){
      return IterOutcome.NONE;
    }
    switch(current.getOutcome()){
      case OK:
      case OK_NEW_SCHEMA:
        vh = new VectorHolder(current.getVectors());
    }
    return current.getOutcome();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return current.getVectors().iterator();
  }
}
