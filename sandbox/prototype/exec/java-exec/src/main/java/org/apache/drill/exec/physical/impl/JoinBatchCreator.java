package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.JoinPOP;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 11:44 AM
 */
public class JoinBatchCreator implements BatchCreator<JoinPOP> {
    @Override
    public RecordBatch getBatch(FragmentContext context, JoinPOP config, List<RecordBatch> children) throws ExecutionSetupException {
        return new JoinBatch(context,config,children.get(0),children.get(1));
    }
}
