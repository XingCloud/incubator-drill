/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.physical.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.physical.config.*;
import org.apache.drill.exec.physical.impl.filter.BufferedBatchCreator;
import org.apache.drill.exec.physical.impl.project.ProjectBatchCreator;
import org.apache.drill.exec.physical.impl.svremover.SVRemoverCreator;
import org.apache.drill.exec.physical.impl.union.UnionBatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.Arrays;
import java.util.List;


public class ImplCreator extends AbstractPhysicalVisitor<RecordBatch, FragmentContext, ExecutionSetupException> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplCreator.class);

    private ScreenCreator sc = new ScreenCreator();
    private RandomReceiverCreator rrc = new RandomReceiverCreator();
    private SingleSenderCreator ssc = new SingleSenderCreator();
    private SVRemoverCreator svc = new SVRemoverCreator();
    private RootExec root = null;
    private BatchCreator<Filter> fc = new BufferedBatchCreator<Filter>(new FilterBatchCreator());
    private BatchCreator<Project> pc = new BufferedBatchCreator<Project>(new ProjectBatchCreator());
  	private UnionBatchCreator unionBatchCreator = new UnionBatchCreator();
    private BatchCreator<SegmentPOP> sgc = new BufferedBatchCreator<SegmentPOP>(new SegmentBatchCreator());
    private BatchCreator<JoinPOP> jc = new BufferedBatchCreator<JoinPOP>(new JoinBatchCreator());
    private BatchCreator<CollapsingAggregatePOP> cac = new BufferedBatchCreator<CollapsingAggregatePOP>(new CollaspsAggreBatchCreator()) ;
    private BatchCreator<Scan> sbc = new BufferedBatchCreator<Scan>(new ScanBatchCreator()) ;


    private ImplCreator() {
    }

    public RootExec getRoot() {
        return root;
    }

    @Override
    public RecordBatch visitProject(Project op, FragmentContext context) throws ExecutionSetupException {
        return pc.getBatch(context, op, getChildren(op, context));
    }

    @Override
    public RecordBatch visitScan(Scan scan, FragmentContext context) throws ExecutionSetupException {
        Preconditions.checkNotNull(scan);
        Preconditions.checkNotNull(context);
        if(scan.getIterationBuffer() == null){
        return sbc.getBatch(context,scan,getChildren(scan,context));
        }
      return sbc.getBatch(context, scan, null);
    }


    @Override
    public RecordBatch visitOp(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
        if (op instanceof SelectionVectorRemover) {
            return svc.getBatch(context, (SelectionVectorRemover) op, getChildren(op, context));
        } else {
            return super.visitOp(op, context);
        }
    }

    @Override
    public RecordBatch visitScreen(Screen op, FragmentContext context) throws ExecutionSetupException {
        Preconditions.checkArgument(root == null);
        root = sc.getRoot(context, op, getChildren(op, context));
        return null;
    }

    @Override
    public RecordBatch visitFilter(Filter filter, FragmentContext context) throws ExecutionSetupException {
     if(filter.getIterationBuffer() == null){
        return fc.getBatch(context, filter, getChildren(filter, context));
      }else{
        return fc.getBatch(context, filter, null);
      }
    }

  @Override
  public RecordBatch visitUnion(Union union, FragmentContext value) throws ExecutionSetupException {
    return unionBatchCreator.getBatch(value, union, getChildren(union, value)); 
  }

  @Override
    public RecordBatch visitSingleSender(SingleSender op, FragmentContext context) throws ExecutionSetupException {
        root = ssc.getRoot(context, op, getChildren(op, context));
        return null;
    }

    @Override
    public RecordBatch visitRandomReceiver(RandomReceiver op, FragmentContext context) throws ExecutionSetupException {
        return rrc.getBatch(context, op, null);
    }


    private List<RecordBatch> getChildren(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
        List<RecordBatch> children = Lists.newArrayList();
        for (PhysicalOperator child : op) {
            children.add(child.accept(this, context));
        }
        return children;
    }

    public static RootExec getExec(FragmentContext context, FragmentRoot root) throws ExecutionSetupException {
        ImplCreator i = new ImplCreator();
        root.accept(i, context);
        if (i.root == null)
            throw new ExecutionSetupException("The provided fragment did not have a root node that correctly created a RootExec value.");
        return i.getRoot();
    }

    @Override
    public RecordBatch visitCollapsingAggregate(CollapsingAggregatePOP op, FragmentContext value) throws ExecutionSetupException {
        return cac.getBatch(value, op, Arrays.asList( op.getChild().accept(this, value)));
    }

    @Override
    public RecordBatch visitSegment(SegmentPOP op, FragmentContext value) throws ExecutionSetupException {
        return sgc.getBatch(value, op, Arrays.asList(op.getChild().accept(this, value)));
    }

    @Override
    public RecordBatch visitJoin(JoinPOP op, FragmentContext value) throws ExecutionSetupException {
        return jc.getBatch(value, op, getChildren(op, value));
    }
}
