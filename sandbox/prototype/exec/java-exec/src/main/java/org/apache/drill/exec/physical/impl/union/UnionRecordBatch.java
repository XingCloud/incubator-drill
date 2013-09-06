package org.apache.drill.exec.physical.impl.union;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Union;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UnionRecordBatch implements RecordBatch {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionRecordBatch.class);

  private final Union unionConfig;
  private final List<RecordBatch> incoming;
  private final FragmentContext context;
  private SelectionVector2 sv;
  private BatchSchema outSchema;
  private List<ValueVector> outputVectors;
  private VectorHolder vh;
  private RecordBatch current = null;
  private List<RecordBatch> childrens = Lists.newArrayList();
  private ArrayList<TransferPair> transfers;
  private int outRecordCount;

  public UnionRecordBatch(Union config, List<RecordBatch> children, FragmentContext context) {
    this.unionConfig = config;
    this.incoming = children;
    childrens.addAll(incoming);
    this.context = context;
    sv = null;
  }


  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    //Preconditions.checkNotNull(outSchema);
    return outSchema;
  }

  @Override
  public int getRecordCount() {
    return outRecordCount;
  }

  @Override
  public void kill() {
    for (RecordBatch batch : incoming) {
      batch.kill();
    }
  }


  @Override
  public Iterator<ValueVector> iterator() {
    return outputVectors.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
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
    if (childrens.isEmpty()) { // end of iteration
      return IterOutcome.NONE;
    }

    IterOutcome upstream = null;
    for (RecordBatch recordBatch : childrens) {
      upstream = recordBatch.next();
      switch (upstream) {
        case OK_NEW_SCHEMA:
        case OK:
          if(recordBatch != current)
            setupSchema();
          current = recordBatch ;
          doTransfer();
          return upstream;
        case NOT_YET:
          continue;
        case NONE:
            childrens.remove(recordBatch);
      }
    }
    return IterOutcome.NOT_YET;

  }

  private void doTransfer() {
    outRecordCount = current.getRecordCount();
    if (outSchema.getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      this.sv = current.getSelectionVector2();
    }
    for (TransferPair transfer : transfers) {
      transfer.transfer();
    }

    for (ValueVector v : this.outputVectors) {
      v.getMutator().setValueCount(outRecordCount);
    }

  }

  private void setupSchema() {
    if (outputVectors != null) {
      for (ValueVector v : outputVectors) {
        v.close();
      }
    }
    this.outputVectors = Lists.newArrayList();
    this.vh = new VectorHolder(outputVectors);
    transfers = Lists.newArrayList();

    for (ValueVector v : current) {
      TransferPair pair = v.getTransferPair();
      outputVectors.add(pair.getTo());
      transfers.add(pair);
    }
    SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(current.getSchema().getSelectionVectorMode());
    for (ValueVector v : outputVectors) {
      bldr.addField(v.getField());
    }
    this.outSchema = bldr.build();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
}
