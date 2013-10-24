package org.apache.drill.exec.engine.async;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.TransferHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class SingleRelayRecordBatch implements RelayRecordBatch {

  static final Logger logger = LoggerFactory.getLogger(SingleRelayRecordBatch.class);

  RecordBatch incoming;
  RecordBatch parent;

  LinkedBlockingDeque<RecordFrame> resultQueue = new LinkedBlockingDeque<>();

  RecordFrame current = new RecordFrame();

  boolean killed = false;

  private VectorHolder vh = null;

  @Override
  public FragmentContext getContext() {
    return current.context;
  }

  @Override
  public BatchSchema getSchema() {
    return current.schema;
  }

  @Override
  public int getRecordCount() {
    return current.recordCount;
  }

  @Override
  public void kill() {
    postCleanup();
    incoming.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return current.sv2;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return current.sv4;
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
    current = resultQueue.poll();
    if(current == null)
      return IterOutcome.NOT_YET ;
    return current.outcome ;
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return current.vectors.iterator();
  }

  @Override
  public void markNextFailed(RuntimeException cause) {
    logger.debug("throwing up errors:{}", cause);
    current.nextErrorCause = cause;
  }


  @Override
  public void postCleanup() {
    killed = true;
    cleanupVectors(current);
    while(!resultQueue.isEmpty()){
      cleanupVectors(resultQueue.poll());
    }
  }

  @Override
  public boolean isKilled() {
    return this.killed;
  }

  @Override
  public void mirrorAndStash(IterOutcome o) {
    RecordFrame recordFrame = mirror(o);
    stash(recordFrame);
  }

  public void stash(RecordFrame recordFrame) {
    try {
      resultQueue.offer(recordFrame, Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public RecordFrame mirror(IterOutcome o) {
    RecordFrame recordFrame = new RecordFrame();
    recordFrame.outcome = o;
    recordFrame.context = incoming.getContext();
    recordFrame.schema = incoming.getSchema();
    switch (o) {
      case OK_NEW_SCHEMA:
      case OK:
        recordFrame.vectors = TransferHelper.mirrorVectors(incoming);
        recordFrame.recordCount = incoming.getRecordCount();
      case NONE:
      case NOT_YET:
      case STOP:
    }
    return recordFrame;
  }

  public void cleanupVectors(RecordFrame current) {
    if (current.vectors != null) {
      for (int i = 0; i < current.vectors.size(); i++) {
        ValueVector vector = current.vectors.get(i);
        vector.clear();
      }
      current.vectors.clear();
    }
  }

  public void setIncoming(RecordBatch incoming) {
    this.incoming = incoming;
  }

  public void setParent(RecordBatch parent) {
    this.parent = parent;
  }
}
