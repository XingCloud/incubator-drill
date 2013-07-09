package org.apache.drill.exec.physical.config;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.FilterBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/3/13
 * Time: 10:18 PM
 */
public class FilterBatchCreator implements BatchCreator<Filter> {
    @Override
    public RecordBatch getBatch(FragmentContext context, Filter config, List<RecordBatch> children) throws ExecutionSetupException {
        return new FilterBatch(context,config,children.get(0));
    }
}
