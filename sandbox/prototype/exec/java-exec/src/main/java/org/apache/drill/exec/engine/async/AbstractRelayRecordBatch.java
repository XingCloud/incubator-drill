package org.apache.drill.exec.engine.async;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.TransferHelper;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/28/13
 * Time: 9:37 AM
 */
public abstract class AbstractRelayRecordBatch implements RelayRecordBatch {
  protected BlockingQueue<RecordFrame> recordFrames;
  protected RecordFrame current;
  protected RecordBatch incoming;
  public RecordBatch parent;

  private final static int STASH_WEAK_LIMIT = 2 ;

  @Override
  public void mirrorAndStash(IterOutcome o) {
    stash(mirror(o));
  }

  private RecordFrame mirror(IterOutcome o) {
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

  @Override
  public boolean nextStash() {
    return recordFrames.size() <= STASH_WEAK_LIMIT;
  }

  protected abstract void stash(RecordFrame recordFrame);

  public void setIncoming(RecordBatch incoming) {
    this.incoming = incoming;
  }

  public void setParent(RecordBatch parent) {
    this.parent = parent;
  }

  @Override
  public void cleanUp() {
    cleanRecordFrame(current);
    RecordFrame recordFrame = null;
    while ((recordFrame = recordFrames.poll()) != null) {
      cleanRecordFrame(recordFrame);
    }
  }

  private void cleanRecordFrame(RecordFrame recordFrame) {
    if (recordFrame == null)
      return;
    if (recordFrame.outcome == IterOutcome.OK_NEW_SCHEMA || recordFrame.outcome == IterOutcome.OK) {
      for (ValueVector v : recordFrame.vectors) {
        v.clear();
      }
    }
  }

  @Override
  public RecordBatch getParent() {
    return parent;
  }

  @Override
  public RecordBatch getIncoming() {
    return incoming;
  }

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
  public SelectionVector2 getSelectionVector2() {
    throw new DrillRuntimeException("Not support yet .");
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new DrillRuntimeException("Not support yet .");
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    throw new DrillRuntimeException("Not support yet .");
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    throw new DrillRuntimeException("Not support yet .");
  }


  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return current.vectors.iterator();
  }
}
