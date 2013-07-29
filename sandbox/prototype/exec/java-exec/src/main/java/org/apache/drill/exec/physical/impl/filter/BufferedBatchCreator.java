package org.apache.drill.exec.physical.impl.filter;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.reentrant.ReentrantOperator;
import org.apache.drill.exec.physical.base.reentrant.ReentrantPhysicalOperator;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.record.buffered.IterationBuffer;

public class BufferedBatchCreator<T extends ReentrantPhysicalOperator> implements BatchCreator<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterBatchCreator.class);

  BatchCreator<T> unbuffered = null;

  public BufferedBatchCreator(BatchCreator<T> unbuffered) {
    this.unbuffered = unbuffered;
  }

  @Override
  public RecordBatch getBatch(FragmentContext context, T config, List<RecordBatch> children) throws ExecutionSetupException {
    if (config.getIterationBuffer() == null){
      config.initIterationBuffer(new IterationBuffer(unbuffered.getBatch(context, config, children)));
    }
    return config.getIterationBuffer().newIteration();
    
  }
  
  
}
