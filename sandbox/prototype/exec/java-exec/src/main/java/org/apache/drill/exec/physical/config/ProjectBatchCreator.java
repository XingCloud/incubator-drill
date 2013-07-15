package org.apache.drill.exec.physical.config;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ProjectBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/3/13
 * Time: 10:15 PM
 */
public class ProjectBatchCreator implements BatchCreator<Project> {

    @Override
    public RecordBatch getBatch(FragmentContext context, Project config, List<RecordBatch> children) throws ExecutionSetupException {
       return new ProjectBatch(context,config,children.get(0));
    }
}
