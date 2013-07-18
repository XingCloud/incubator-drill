package org.apache.drill.exec.physical.config;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.SegmentBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 11:43 AM
 */
public class SegmentBatchCreator implements BatchCreator<Group> {
    @Override
    public RecordBatch getBatch(FragmentContext context, Group config, List<RecordBatch> children) throws ExecutionSetupException {
        return new SegmentBatch(context,config,children.get(0));
    }
}
