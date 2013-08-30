package org.apache.drill.exec.physical.config;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.CollapsingAggregateBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/2/13
 * Time: 11:51 AM
 */
public class CollaspsAggreBatchCreator implements BatchCreator<CollapsingAggregatePOP> {
  @Override
  public RecordBatch getBatch(FragmentContext context, CollapsingAggregatePOP config, List<RecordBatch> children) throws ExecutionSetupException {
   return new CollapsingAggregateBatch(context, config, children.get(0));
  }
}
