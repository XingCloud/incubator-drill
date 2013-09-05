package org.apache.drill.exec.engine.async;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SingleRelayRecordBatch implements RelayRecordBatch {


  RecordBatch incoming;
  RecordBatch parent;
  
  RecordFrame my = new RecordFrame();
 
  boolean killed = false;
  
  private VectorHolder vh = null;

  @Override
  public FragmentContext getContext() {
    return getCurrent().context;
  }

  @Override
  public BatchSchema getSchema() {
    return getCurrent().schema;
  }

  @Override
  public int getRecordCount() {
    return getCurrent().recordCount;
  }

  @Override
  public void kill() {
    killed = true;
    cleanupVectors(getCurrent());
    incoming.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return getCurrent().sv2;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return getCurrent().sv4;
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return vh.getValueVector(path);
  }

  @Override
  public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
    return vh.getValueVector(fieldId, clazz);
  }

  public RecordFrame getCurrent() {
    return my;
  }

  @Override
  public IterOutcome next() {
    if(null==getCurrent().outcome){
      return IterOutcome.NOT_YET;
    }
    IterOutcome ret = getCurrent().outcome;
    getCurrent().outcome = null;
    return ret;
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return getCurrent().vectors.iterator();
  }
  
  @Override
  public void mirrorResultFromIncoming(IterOutcome incomingOutcome){
    mirrorResultFromIncoming(incomingOutcome, incoming, getCurrent());
    if(incomingOutcome == IterOutcome.OK_NEW_SCHEMA){
      vh = new VectorHolder(getCurrent().vectors);
    }
  }

  @Override
  public boolean isKilled() {
    return this.killed;
  }

  protected void mirrorResultFromIncoming(IterOutcome incomingOutcome, RecordBatch incoming, RecordFrame current) {
    current.outcome = incomingOutcome;
    switch (current.outcome) {
      case OK_NEW_SCHEMA:
        current.schema = incoming.getSchema();
        cleanupVectors(current);
        for (ValueVector vector : incoming) {
          TransferPair tp = vector.getTransferPair();
          current.vectors.add(tp.getTo());
        }
        //fall through
      case OK:
        int i = 0;
        for (ValueVector vector : incoming) {
          ValueVector currentVector = current.vectors.get(i++);
          vector.getMutator().transferTo(currentVector, false);
        }
        switch (current.schema.getSelectionVectorMode()) {
          case NONE:
            break;
          case TWO_BYTE:
            current.sv2 = incoming.getSelectionVector2();
            break;
          case FOUR_BYTE:
            current.sv4 = incoming.getSelectionVector4();
            break;
          default:
            throw new UnsupportedOperationException("SV no recognized!" + current.schema.getSelectionVectorMode());
        }
        current.recordCount = incoming.getRecordCount();
        break;
      case STOP:
        cleanupVectors(current);
        break;
      default:
        break;
    }
    
  }

  public void cleanupVectors(RecordFrame current){
    if(current.vectors!=null){
      for (int i = 0; i < current.vectors.size(); i++) {
        ValueVector vector = current.vectors.get(i);
        vector.clear();
      }
    }
    current.vectors = new ArrayList<>();
    if(current.sv2 != null){
      current.sv2.clear();
      current.sv2 = null;
    }
    if(current.sv4 != null){
      //todo sv4 clear
      current.sv4 = null;
    }
  }

  public void setIncoming(RecordBatch incoming) {
    this.incoming = incoming;
  }

  public void setParent(RecordBatch parent) {
    this.parent = parent;
  }
}
