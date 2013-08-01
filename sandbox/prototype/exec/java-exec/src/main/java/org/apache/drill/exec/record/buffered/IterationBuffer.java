package org.apache.drill.exec.record.buffered;

import com.google.common.collect.Lists;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.List;

/**
 * recording & replaying result sequence of a RecordBatch's next().
 */
public class IterationBuffer {

  private List<BufferedStackFrameImpl> buffer = new ArrayList<>();

  private int totalIterations = 0;

  private RecordBatch head = null;
  private BatchSchema headSchema = null;

  private RecordBatch.IterOutcome iterationEndState = null;

  public IterationBuffer(RecordBatch head) {
    this.head = head;
  }

  public synchronized BufferedRecordBatch newIteration() {
    totalIterations++;
    return new BufferedRecordBatch(this);
  }

  public boolean forward(BufferedRecordBatch recordBatch) {
    StackFrameDelegate current = (StackFrameDelegate) recordBatch.current;
    if (current.currentPos >= 0) {
      buffer.get(current.currentPos).leave(recordBatch);
    }
    if (current.currentPos == buffer.size() - 1) {//came to buffer end
      if (iterationEndState != null) {
        return false;//really end
      } else {// go on with head
        forwardHead();
        if (current.currentPos == buffer.size() - 1) {//forward failed
          return false;
        }
      }
    }
    current.currentPos++;
    BufferedStackFrameImpl newFrame = buffer.get(current.currentPos);
    newFrame.enter(recordBatch);
    //if IterOutcome.NONE frame reached, there would be no forward() invoked
    //so leave this frame early
    if (newFrame.getOutcome() == RecordBatch.IterOutcome.NONE
      || newFrame.getOutcome() == RecordBatch.IterOutcome.STOP) {
      if (current.currentPos != buffer.size() - 1) {
        throw new IllegalStateException(newFrame.getOutcome() + " frame not the last frame!");
      }
      newFrame.leave(recordBatch);
    }
    return true;
  }

  private void forwardHead() {
    RecordBatch.IterOutcome outcome = head.next();
    switch (outcome) {
      case OK_NEW_SCHEMA:
        setupSchema();
      case OK:
        doTransfer(outcome);
        break;
      default:
        BufferedStackFrameImpl frame = new BufferedStackFrameImpl(head.getSchema(),
          0, null, null, null,
          outcome, head.getContext());
        buffer.add(frame);
        if (outcome == RecordBatch.IterOutcome.STOP || outcome == RecordBatch.IterOutcome.NONE) {
          iterationEndState = outcome;
        }
        break;
    }
  }

  private void setupSchema() {
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(head.getSchema().getSelectionVectorMode());
    for (ValueVector v : head) {
      bldr.addField(v.getField());
    }
    this.headSchema = bldr.build();
  }


  private void doTransfer(RecordBatch.IterOutcome outcome) {
    int outRecordCount = head.getRecordCount();
    SelectionVector2 sv2 = null;
    SelectionVector4 sv4 = null;
    if (headSchema.getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      sv2 = head.getSelectionVector2();
    }
    ArrayList<ValueVector> vectors = new ArrayList<>();
    for (ValueVector v : head) {
      TransferPair tp = v.getTransferPair();
      vectors.add(tp.getTo());
      tp.transfer();
    }
    for (ValueVector v : vectors) {
      v.getMutator().setValueCount(outRecordCount);
    }
    BufferedStackFrameImpl frame = new BufferedStackFrameImpl(headSchema,
      outRecordCount, sv2, sv4, vectors,
      outcome, head.getContext());
    buffer.add(frame);
  }

  public void abort(BufferedRecordBatch recordBatch) {
    StackFrameDelegate current = (StackFrameDelegate) recordBatch.current;
    current.delegate.leave(recordBatch);
    totalIterations--;
    for (int i = current.currentPos + 1; i < buffer.size(); i++) {
      BufferedStackFrameImpl frame = buffer.get(i);
      if (frame.currentIterations == totalIterations) {
        frame.close();
      } else {
        break;
      }
    }
  }

  public void init(BufferedRecordBatch recordBatch) {
    recordBatch.current = new StackFrameDelegate(null);
  }

  public void setHead(RecordBatch head) {
    this.head = head;
  }

  public class BufferedStackFrameImpl implements BufferedStackFrame {

    /**
     * recorded iteration results
     */
    BatchSchema schema;
    private int recordCount;
    private SelectionVector2 selectionVector2;
    private SelectionVector4 selectionVector4;
    private List<ValueVector> vectors;
    private RecordBatch.IterOutcome outcome;
    private FragmentContext context;

    /**
     * reference count for BufferedRecordBatch
     */

    private int currentIterations = 0;

    public BufferedStackFrameImpl(BatchSchema schema,
                                  int recordCount,
                                  SelectionVector2 selectionVector2,
                                  SelectionVector4 selectionVector4,
                                  List<ValueVector> vectors,
                                  RecordBatch.IterOutcome outcome,
                                  FragmentContext context) {
      this.schema = schema;
      this.recordCount = recordCount;
      this.selectionVector2 = selectionVector2;
      this.selectionVector4 = selectionVector4;
      this.vectors = vectors;
      this.outcome = outcome;
      this.context = context;
    }

    public void enter(BufferedRecordBatch recordBatch) {
      if (currentIterations >= totalIterations) {
        throw new IllegalArgumentException("expectedIteration:" + totalIterations + ",now:" + currentIterations);
      }
      ((StackFrameDelegate) recordBatch.current).setDelegate(this);
    }

    public void leave(BufferedRecordBatch recordBatch) {
      currentIterations++;
      if (currentIterations == totalIterations) {
        close();
      } else if (currentIterations > totalIterations) {
        //check
        throw new IllegalArgumentException("expectedIteration:" + totalIterations + ",now:" + currentIterations);
      }
    }

    private void close() {
      //clear self
      this.selectionVector2 = null;
      this.selectionVector4 = null;
      if (vectors != null) {
        for (ValueVector v : vectors) {
          v.close();
        }
        this.vectors.clear();
        this.vectors = null;
      }
    }

    @Override
    public BatchSchema getSchema() {
      return schema;
    }

    @Override
    public int getRecordCount() {
      return recordCount;
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return selectionVector2;
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      return selectionVector4;
    }

    @Override
    public List<ValueVector> getVectors() {
      return vectors;
    }

    @Override
    public RecordBatch.IterOutcome getOutcome() {
      return outcome;
    }

    public FragmentContext getContext() {
      return context;
    }
  }

  public class StackFrameDelegate implements BufferedStackFrame {

    BufferedStackFrameImpl delegate;

    List<ValueVector> mirroredVectors;

    int currentPos = -1;

    public StackFrameDelegate(BufferedStackFrameImpl delegate) {
      if (delegate != null) setDelegate(delegate);
    }

    public void setDelegate(BufferedStackFrameImpl delegate) {
      this.delegate = delegate;
      switch (delegate.getOutcome()) {
        case OK_NEW_SCHEMA:
          //set up new vectors
          if (this.mirroredVectors != null) {
            for (ValueVector v : mirroredVectors) {
              v.close();
            }
          }
          this.mirroredVectors = new ArrayList<>();
          if (delegate.getVectors() != null) {
            for (ValueVector v : delegate.getVectors()) {
              TransferPair tp = v.getTransferPair();
              tp.mirror();
              mirroredVectors.add(tp.getTo());
            }
          }
          break;
        case OK:
          List<ValueVector> bufferedVectors = delegate.getVectors();
          for (int i = 0; i < mirroredVectors.size(); i++) {
            ValueVector mirrored = mirroredVectors.get(i);
            ValueVector buffered = bufferedVectors.get(i);
            buffered.getMutator().transferTo(mirrored, false);
          }
          break;
        default:
          break;
      }
    }

    @Override
    public BatchSchema getSchema() {
      return delegate.getSchema();
    }

    @Override
    public int getRecordCount() {
      return delegate.getRecordCount();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return delegate.getSelectionVector2();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      return delegate.getSelectionVector4();
    }

    @Override
    public List<ValueVector> getVectors() {
      return mirroredVectors;
    }

    @Override
    public RecordBatch.IterOutcome getOutcome() {
      return delegate.getOutcome();
    }

    @Override
    public FragmentContext getContext() {
      return delegate.getContext();
    }
  }
}
