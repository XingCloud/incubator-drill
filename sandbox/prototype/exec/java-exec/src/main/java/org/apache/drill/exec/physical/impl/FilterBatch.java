package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.record.BaseRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/2/13
 * Time: 11:02 AM
 */
public class FilterBatch extends BaseRecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterBatch.class);

  private FragmentContext context;
  private Filter config;
  private RecordBatch incoming;
  private BooleanEvaluator eval;
  private boolean new_schema = true;
  private BitVector bitVector = null;


  public FilterBatch(FragmentContext context, Filter config, RecordBatch incoming) {
    this.context = context;
    this.config = config;
    this.incoming = incoming;
    setupEvals();
  }

  @Override
  public void setupEvals() {
    eval = new BasicEvaluatorFactory().getBooleanEvaluator(incoming, config.getExpr());
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return incoming.getSchema();
  }

  @Override
  public void kill() {
    releaseAssets();
    incoming.kill();
  }

  @Override
  public IterOutcome next() {

    while (true) {
      IterOutcome o = incoming.next();
      switch (o) {
        case NONE:
        case STOP:
        case NOT_YET:
          recordCount = 0;
          break;
        case OK_NEW_SCHEMA:
          new_schema = true;
        case OK:
          recordCount = 0;
          outputVectors.clear();
          try {
            bitVector = eval.eval();
            BitVector.Accessor bitFilter = bitVector.getAccessor();
            for (int i = 0; i < bitFilter.getValueCount(); i++) {
              if (bitFilter.get(i) == 1) {
                recordCount++;
              }
            }
            if (recordCount == 0) {
              clearBits();
              clearIncoming();
              continue;
            }
            for (ValueVector in : incoming) {
              ValueVector out = TypeHelper.getNewVector(in.getField(), context.getAllocator());
              AllocationHelper.allocate(out, recordCount, 8);
              ValueVector.Mutator mutator = out.getMutator();
              ValueVector.Accessor accessor = in.getAccessor();
              for (int i = 0, j = 0; i < recordCount && j < accessor.getValueCount(); j++) {
                if (bitFilter.get(j) == 1) {
                  mutator.setObject(i++, accessor.getObject(j));
                }
              }
              mutator.setValueCount(recordCount);
              outputVectors.add(out);
            }
            clearIncoming();
            clearBits();
            vh = new VectorHolder(outputVectors);
          } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            context.fail(e);
            return IterOutcome.STOP;
          }
          break;

      }
      if (new_schema) {
        new_schema = false;
        return IterOutcome.OK_NEW_SCHEMA;
      }
      return o;
    }

  }

  @Override
  public void releaseAssets() {
    for (ValueVector v : outputVectors) {
      v.close();
    }
    clearBits();
  }

  private void clearIncoming(){
    for(ValueVector v : incoming){
      v.close();
    }
  }

  private void clearBits() {
    if (bitVector != null) {
      bitVector.close();
      bitVector = null;
    }
  }
}
