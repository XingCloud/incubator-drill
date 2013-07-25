package org.apache.drill.exec.record.buffered;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
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
  
  private Boolean supportSV2 = null;
  private Boolean supportSV4 = null;
  
  private RecordBatch.IterOutcome iterationEndState = null;

  public IterationBuffer(RecordBatch head) {
    this.head = head;
  }

  public synchronized BufferedRecordBatch newIteration(){
    totalIterations++;
    return new BufferedRecordBatch(this);
  }

  public boolean forward(BufferedRecordBatch recordBatch) {
    StackFrameDelegate current = (StackFrameDelegate) recordBatch.current;
    if(current.currentPos == buffer.size()-1){//came to buffer end
      if(iterationEndState != null){
        return false;//really end
      }else{// go on with head
        forwardHead();
        if(current.currentPos == buffer.size()-1){//forward failed
          return false;
        }
      }
    }
    if(current.currentPos >= 0){
      buffer.get(current.currentPos).leave(recordBatch);
    }
    current.currentPos++;
    BufferedStackFrameImpl newStack = buffer.get(current.currentPos);
    newStack.enter(recordBatch);
    current.setDelegate(newStack);
    return true;
  }

  private void forwardHead() {
    RecordBatch.IterOutcome outcome = head.next();
    switch(outcome){
      case OK_NEW_SCHEMA:
      case OK:
        BufferedStackFrameImpl frame = new BufferedStackFrameImpl(head.getSchema(),
          head.getRecordCount(), pickSV2(head), pickSV4(head), fetchVectors(head),
          outcome, head.getContext());
        buffer.add(frame);
        break;
      default:
        frame = new BufferedStackFrameImpl(head.getSchema(),
          0, null, null, null,
          outcome, head.getContext()); 
        buffer.add(frame);
        iterationEndState = outcome;
        break;
    }
  }

  private List<ValueVector> fetchVectors(RecordBatch head) {
    ArrayList<ValueVector> vectors = new ArrayList<>();
    for(ValueVector v:head){
      TransferPair tp = v.getTransferPair();
      vectors.add(tp.getTo());
      tp.transfer();
    }
    return vectors;
  }

  private SelectionVector2 pickSV2(RecordBatch head) {
    if(supportSV2 == null){
      try{
        SelectionVector2 sv2 = head.getSelectionVector2();
        supportSV2 = true;
        return sv2;
      }catch(UnsupportedOperationException e){
        supportSV2 = false;
        return null;
      }
    }else if(supportSV2){
      return head.getSelectionVector2();
    }else{
      return null;
    }
  }

  private SelectionVector4 pickSV4(RecordBatch head) {
    if(supportSV4 == null){
      try{
        SelectionVector4 sv4 = head.getSelectionVector4();
        supportSV4 = true;
        return sv4;
      }catch(UnsupportedOperationException e){
        supportSV4 = false;
        return null;
      }
    }else if(supportSV4){
      return head.getSelectionVector4();
    }else{
      return null;
    }
  }

  public void abort(BufferedRecordBatch recordBatch) {
    StackFrameDelegate current = (StackFrameDelegate) recordBatch.current;
    current.delegate.leave(recordBatch);
    totalIterations--;
    for(int i = current.currentPos+1; i< buffer.size();i++){
      BufferedStackFrameImpl frame = buffer.get(i);
      if(frame.currentIterations == totalIterations){
         frame.close();
      }else{
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
    
    public void enter(BufferedRecordBatch recordBatch){
      if(currentIterations >= totalIterations){
        throw new IllegalArgumentException("expectedIteration:"+totalIterations+",now:"+currentIterations);
      }
    }
    
    public void leave(BufferedRecordBatch recordBatch){
      currentIterations ++;
      if(currentIterations == totalIterations){
        close();
      }else if(currentIterations > totalIterations){
        //check
        throw new IllegalArgumentException("expectedIteration:"+totalIterations+",now:"+currentIterations);        
      }
    }

    private void close() {
        //clear self
        this.selectionVector2 = null;
        this.selectionVector4 = null;
        for(ValueVector v:vectors){
          v.close();
        }
        this.vectors.clear();
        this.vectors = null;
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
  
  public class StackFrameDelegate implements BufferedStackFrame{

    BufferedStackFrameImpl delegate;

    List<ValueVector> mirroredVectors;

    int currentPos = -1;
    
    public StackFrameDelegate(BufferedStackFrameImpl delegate) {
      if(delegate != null) setDelegate(delegate);
    }

    public void setDelegate(BufferedStackFrameImpl delegate) {
      this.delegate = delegate;
      this.mirroredVectors = new ArrayList<>();
      if(delegate.getVectors()!=null){
        for(ValueVector v:delegate.getVectors()){
          TransferPair tp = v.getTransferPair();
          tp.mirror();
          mirroredVectors.add(tp.getTo());
        }
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
