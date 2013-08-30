package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.impl.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.record.*;
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
public class FilterBatch extends AbstractSingleRecordBatch {
  
  public FilterBatch(PhysicalOperator popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, incoming);
    
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    //TODO method implementation
  }

  @Override
  protected void doWork() {
    //TODO method implementation
  }

  @Override
  public int getRecordCount() {
    return 0;  //TODO method implementation
  }
  /*
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterBatch.class);

  private Filter config;
  private RecordBatch incoming;
  private BooleanEvaluator eval;
  private boolean new_schema = true;
  private BitVector bitVector = null;


  public FilterBatch(FragmentContext context, Filter config, RecordBatch incoming) {
    super(config, context);
    this.config = config;
    this.incoming = incoming;
    setupEvals();
  }

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
  public int getRecordCount() {
    return 0;  //TODO method implementation
  }

  @Override
  public void kill() {
    releaseAssets();
    incoming.kill();
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    
    return null;  //TODO method implementation
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
            for (VectorWrapper<?> in : incoming) {
              ValueVector out = TypeHelper.getNewVector(in.getField(), context.getAllocator());
              AllocationHelper.allocate(out, recordCount, 8);
              ValueVector.Mutator mutator = out.getMutator();
              ValueVector.Accessor accessor = in.getValueVector().getAccessor();
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
  protected void setupNewSchema() throws SchemaChangeException {
    
    //TODO method implementation
  }

  @Override
  protected void doWork() {
    //TODO method implementation
  }

  public void releaseAssets() {
    for (VectorWrapper<?> v : container) {
      v.release();
    }
    clearBits();
  }

  private void clearIncoming(){
    for(VectorWrapper<?> v : incoming){
      v.release();
    }
  }

  private void clearBits() {
    if (bitVector != null) {
      bitVector.close();
      bitVector = null;
    }
  }
  */
}
