package org.apache.drill.exec.physical.impl.unionedscan;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionedScanSplitPOP;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Iterator;

public class UnionedScanSplitBatch implements RecordBatch{

  private final UnionedScanSplitPOP pop;
  private final FragmentContext context;
  private final UnionedScanBatch unionedScan;
  
  ScanMode scanMode = null;
  public int currentIndex;
  public int currentOutput;

  public UnionedScanSplitBatch(FragmentContext context, UnionedScanSplitPOP config, UnionedScanBatch unionedScan) {
    this.pop = config;
    this.context = context;
    this.unionedScan = unionedScan;
  }

  @Override
  public FragmentContext getContext() {
    return this.context;
  }

  @Override
  public BatchSchema getSchema() {
    return unionedScan.getSchema(this);
  }

  @Override
  public int getRecordCount() {
    return unionedScan.getRecordCount(this);
  }

  @Override
  public void kill() {
    unionedScan.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return null;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return null;
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return unionedScan.getValueVectorId(this, path);
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    return unionedScan.getValueVectorById(this, fieldId, clazz);
  }

  @Override
  public IterOutcome next() {
    //first next() to decide scanMode
    if(scanMode == null){
      unionedScan.initSplitScan(this);
    }
    return unionedScan.next(this);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return unionedScan.iterator(this);
  }

  static enum ScanMode {
    direct, cached
  }
}
