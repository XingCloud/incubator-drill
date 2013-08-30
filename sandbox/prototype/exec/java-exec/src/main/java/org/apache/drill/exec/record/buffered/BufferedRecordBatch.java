package org.apache.drill.exec.record.buffered;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class BufferedRecordBatch implements RecordBatch {
  static final Logger logger = LoggerFactory.getLogger(BufferedRecordBatch.class);

  private final IterationBuffer iterationBuffer;

  BufferedStackFrame current;
  
  private VectorContainer vc;
  
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
    return vc.getValueVector(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    VectorWrapper<? extends ValueVector> v = current.getVectors().get(fieldId);
    assert v != null;
    if (v.getVectorClass() != clazz) {
      logger.warn(String.format(
          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s.",
          clazz.getCanonicalName(), v.getVectorClass().getCanonicalName()));
      return null;
    }
    return v;
  }

  @Override
  public IterOutcome next() {
    if(!iterationBuffer.forward(this)){
      return IterOutcome.NONE;
    }
    switch(current.getOutcome()){
      case OK_NEW_SCHEMA:
        vc = new VectorContainer(current.getVectors());
      case OK:
    }
    return current.getOutcome();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return current.getVectors().iterator();
  }
}
