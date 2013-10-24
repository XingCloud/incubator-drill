package org.apache.drill.exec.physical.impl.union;

import com.google.common.collect.Lists;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Union;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.TransferHelper;
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
  private List<RecordBatch> children = Lists.newArrayList();
  private ArrayList<TransferPair> transfers;
  private int outRecordCount;

  public UnionRecordBatch(Union config, List<RecordBatch> children, FragmentContext context) {
    this.unionConfig = config;
    this.incoming = children;
    this.children.addAll(incoming);
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
    if (children.isEmpty()) { // end of iteration
      return IterOutcome.NONE;
    }

    IterOutcome upstream = null;
    for (Iterator<RecordBatch> it = children.iterator();it.hasNext();) {
      RecordBatch recordBatch = it.next();
      upstream = recordBatch.next();
      switch (upstream) {
        case OK_NEW_SCHEMA:
        case OK:
          if (upstream == IterOutcome.OK_NEW_SCHEMA || recordBatch != current) {
            current = recordBatch;
            upstream = IterOutcome.OK_NEW_SCHEMA;
            doTransfer();
            setupSchema();
          } else{
            doTransfer();
          }
          return upstream;
        case NOT_YET:
          continue;
        case STOP:
          return IterOutcome.STOP;
        case NONE:
          it.remove();
      }
    }
    if(children.size()==0){
      return IterOutcome.NONE;
    }
    return IterOutcome.NOT_YET;

  }

  private void doTransfer() {
    outRecordCount = current.getRecordCount();
    this.outputVectors = TransferHelper.transferVectors(current);
  }

  private void setupSchema() {
    SchemaBuilder bldr = BatchSchema.newBuilder();
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
