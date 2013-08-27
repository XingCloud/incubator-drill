package org.apache.drill.exec.physical.impl.unionedscan;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.UnionedScanSplitPOP;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Iterator;
import java.util.List;

public class UnionedScanBatch implements RecordBatch {

  /**
   * entry id 就是entry在original entries 里面的序号。
   * 将original Entry 重新排序，形成真正的entry
   */
  private final List<HbaseScanPOP.HbaseScanEntry> originalEntries;
  private FragmentContext context;

  private MultiEntryHBaseRecordReader reader = null;
  private int currentEntry;

  public UnionedScanBatch(FragmentContext context, List<HbaseScanPOP.HbaseScanEntry> readEntries) {
    this.context = context;
    this.originalEntries  = readEntries;
    sortEntries();
    initReader();
  }

  private void initReader() {
    //TODO method implementation
  }

  private void sortEntries() {
    //TODO method implementation
  }

  public RecordBatch createSplitBatch(FragmentContext context, UnionedScanSplitPOP config) {
    return new UnionedScanSplitBatch(context, config, this);
  }
  
  @Override
  public FragmentContext getContext() {
    return context;
  }

  public void initSplitScan(UnionedScanSplitBatch scanSplit) {
    
    //TODO method implementation
  }

  public BatchSchema getSchema(UnionedScanSplitBatch scanSplit) {
    return null;  //TODO method implementation
  }

  public int getRecordCount(UnionedScanSplitBatch scanSplit) {
    return 0;  //TODO method implementation
  }

  public TypedFieldId getValueVectorId(UnionedScanSplitBatch scanSplit, SchemaPath path) {
    return null;  //TODO method implementation
  }

  public <T extends ValueVector> T getValueVectorById(UnionedScanSplitBatch scanSplit, int fieldId, Class<?> clazz) {
    return null;  //TODO method implementation
  }

  public IterOutcome next(UnionedScanSplitBatch scanSplit) {
    if(scanSplit.currentIndex > currentEntry){
      //read ahead and buffer
      forwardReaderTo(scanSplit.currentIndex);
    }else if(scanSplit.currentIndex == currentEntry){
      return reader.next();
    }else{//scanSplit.currentIndex < currentEntry;
      return buffer.get(scanSplit.currentIndex).get(scanSplit.currentOutput).getOutCome();
    }
    return null;  //TODO method implementation
  }

  public Iterator<ValueVector> iterator(UnionedScanSplitBatch scanSplit) {
    if(scanSplit.currentIndex == currentEntry){
      return reader.outputVectors();
    }else{
      return buffer.get(scanSplit.currentIndex).get(scanSplit.currentOutput).valueVectorIterator();      
    }
  }

  @Override
  public void kill() {
    reader.cleanup();
  }

  @Override
  public BatchSchema getSchema() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public int getRecordCount() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");    
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public IterOutcome next() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

  @Override
  public Iterator<ValueVector> iterator() {
    throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
  }

}
