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
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.physical.config.RandomReceiver;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.physical.config.Union;
import org.apache.drill.exec.physical.impl.aggregate.AggBatchCreator;
import org.apache.drill.exec.physical.impl.filter.FilterBatchCreator;
import org.apache.drill.exec.physical.impl.join.MergeJoinCreator;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderCreator;
import org.apache.drill.exec.physical.config.*;
import org.apache.drill.exec.physical.impl.filter.BufferedBatchCreator;
import org.apache.drill.exec.physical.impl.project.ProjectBatchCreator;
import org.apache.drill.exec.physical.impl.sort.SortBatchCreator;
import org.apache.drill.exec.physical.impl.svremover.SVRemoverCreator;
import org.apache.drill.exec.physical.impl.union.UnionBatchCreator;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanBatchCreator;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanSplitBatchCreator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.json.JSONScanBatchCreator;
import org.apache.drill.exec.store.json.JSONSubScan;
import org.apache.drill.exec.store.mock.MockScanBatchCreator;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.apache.drill.exec.store.parquet.ParquetRowGroupScan;
import org.apache.drill.exec.store.parquet.ParquetScanBatchCreator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Implementation of the physical operator visitor
 */
public class ImplCreator extends AbstractPhysicalVisitor<RecordBatch, FragmentContext, ExecutionSetupException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplCreator.class);

  // Element creators supported by this visitor
  
  private MockScanBatchCreator msc = new MockScanBatchCreator();
  private ParquetScanBatchCreator parquetScan = new ParquetScanBatchCreator();
  private ScreenCreator sc = new ScreenCreator();
  private RandomReceiverCreator rrc = new RandomReceiverCreator();
  private PartitionSenderCreator hsc = new PartitionSenderCreator();
  private SingleSenderCreator ssc = new SingleSenderCreator();
//  private ProjectBatchCreator pbc = new ProjectBatchCreator();
//  private FilterBatchCreator fbc = new FilterBatchCreator();
  private UnionBatchCreator unionbc = new UnionBatchCreator();
  private BatchCreator<SelectionVectorRemover>  svc = new BufferedBatchCreator<SelectionVectorRemover>(new SVRemoverCreator());
  private SortBatchCreator sortbc = new SortBatchCreator();
  private AggBatchCreator abc = new AggBatchCreator();
  private MergeJoinCreator mjc = new MergeJoinCreator();
  private RootExec root = null;
  private BatchCreator<Filter> fc = new BufferedBatchCreator<Filter>(new FilterBatchCreator());
  private BatchCreator<Project> pc = new BufferedBatchCreator<Project>(new ProjectBatchCreator());
  private BatchCreator<SegmentPOP> sgc = new BufferedBatchCreator<SegmentPOP>(new SegmentBatchCreator());
  private BatchCreator<JoinPOP> jc = new BufferedBatchCreator<JoinPOP>(new JoinBatchCreator());
  private BatchCreator<CollapsingAggregatePOP> cac = new BufferedBatchCreator<CollapsingAggregatePOP>(new CollaspsAggreBatchCreator());
  private BatchCreator<Union> uc = new BufferedBatchCreator<Union>(new UnionBatchCreator());
  private BatchCreator<UnionedScanSplitPOP> splitc = new BufferedBatchCreator<UnionedScanSplitPOP>(new UnionedScanSplitBatchCreator());
  private UnionedScanBatchCreator unionedScanc = new UnionedScanBatchCreator();
  private BatchCreator<SubScan> sbc = new BufferedBatchCreator<SubScan>(new ScanBatchCreator());


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
  public RecordBatch visitSubScan(SubScan subScan, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkNotNull(subScan);
    Preconditions.checkNotNull(context);
    BatchCreator bc = null;
    if (subScan instanceof HbaseScanPOP){
      bc = sbc;
    }else if(subScan instanceof MysqlScanPOP){
      bc = sbc;
    }else if (subScan instanceof UnionedScanPOP){
      bc = unionedScanc;
    } else if (subScan instanceof MockSubScanPOP) {
      bc = msc;     
    } else if (subScan instanceof JSONSubScan) {
      bc = new JSONScanBatchCreator();
    } else if (subScan instanceof ParquetRowGroupScan) {
      bc = parquetScan;
    } else {
      return super.visitSubScan(subScan, context);
    }
    return bc.getBatch(context, subScan, Collections.<RecordBatch> emptyList());
  }

  @Override
  public RecordBatch visitOp(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    if (op instanceof SelectionVectorRemover) {
      if(((SelectionVectorRemover) op).getIterationBuffer()==null){
        return svc.getBatch(context, (SelectionVectorRemover) op, getChildren(op, context));
      }else{
        return svc.getBatch(context, (SelectionVectorRemover) op, null);
      }
    } else {
      return super.visitOp(op, context);
    }
  }

  @Override
  public RecordBatch visitUnionedScanSplit(UnionedScanSplitPOP scan, FragmentContext context) throws ExecutionSetupException {
    if(scan.getIterationBuffer() == null){
    return splitc.getBatch(context, scan, getChildren(scan, context));
    }else{
      return splitc.getBatch(context, scan, null);
    }
  }

  @Override
  public RecordBatch visitSort(Sort sort, FragmentContext context) throws ExecutionSetupException {
    return sortbc.getBatch(context, sort, getChildren(sort, context));
  }

  @Override
  public RecordBatch visitMergeJoin(MergeJoinPOP op, FragmentContext context) throws ExecutionSetupException {
    return mjc.getBatch(context, op, getChildren(op, context));

  }

  @Override
  public RecordBatch visitScreen(Screen op, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkArgument(root == null);
    root = sc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitHashPartitionSender(HashPartitionSender op, FragmentContext context) throws ExecutionSetupException {
    root = hsc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitFilter(Filter filter, FragmentContext context) throws ExecutionSetupException {
    if (filter.getIterationBuffer() == null)
      return fc.getBatch(context, filter, getChildren(filter, context));
    return fc.getBatch(context, filter, null);
  }
  
  @Override
  public RecordBatch visitStreamingAggregate(StreamingAggregate config, FragmentContext context)
      throws ExecutionSetupException {
    return abc.getBatch(context, config, getChildren(config, context));
  }

  @Override
  public RecordBatch visitUnion(Union union, FragmentContext value) throws ExecutionSetupException {
    if (union.getIterationBuffer() == null)
      return uc.getBatch(value, union, getChildren(union, value));
    return uc.getBatch(value,union,null);
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
      throw new ExecutionSetupException(
          "The provided fragment did not have a root node that correctly created a RootExec value.");
    return i.getRoot();
  }



  @Override
  public RecordBatch visitCollapsingAggregate(CollapsingAggregatePOP op, FragmentContext value) throws ExecutionSetupException {
    if (op.getIterationBuffer() == null)
      return cac.getBatch(value, op, Arrays.asList(op.getChild().accept(this, value)));
    return cac.getBatch(value,op,null);
  }

  @Override
  public RecordBatch visitSegment(SegmentPOP op, FragmentContext value) throws ExecutionSetupException {
    if (op.getIterationBuffer() == null)
      return sgc.getBatch(value, op, Arrays.asList(op.getChild().accept(this, value)));
    return sgc.getBatch(value,op,null);
  }

  @Override
  public RecordBatch visitJoin(JoinPOP op, FragmentContext value) throws ExecutionSetupException {
    if (op.getIterationBuffer() == null)
      return jc.getBatch(value, op, getChildren(op, value));
    return jc.getBatch(value, op, null);
  }
}
