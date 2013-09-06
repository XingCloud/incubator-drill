package org.apache.drill.exec.engine.async;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.config.*;
import org.apache.drill.exec.physical.impl.*;
import org.apache.drill.exec.physical.impl.project.ProjectBatchCreator;
import org.apache.drill.exec.physical.impl.svremover.SVRemoverCreator;
import org.apache.drill.exec.physical.impl.union.UnionBatchCreator;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanBatchCreator;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanSplitBatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class AsyncImplCreator extends AbstractPhysicalVisitor<RecordBatch, FragmentContext, ExecutionSetupException> {
  //TODO
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AsyncImplCreator.class);

  private ScreenCreator sc = new ScreenCreator();
  private RandomReceiverCreator rrc = new RandomReceiverCreator();
  private SingleSenderCreator ssc = new SingleSenderCreator();
  private SVRemoverCreator svc = new SVRemoverCreator();
  private RootExec root = null;
  private BatchCreator<Filter> fc = new FilterBatchCreator();
  private BatchCreator<Project> pc = new ProjectBatchCreator();
  private BatchCreator<SegmentPOP> sgc = new SegmentBatchCreator();
  private BatchCreator<JoinPOP> jc = new JoinBatchCreator();
  private BatchCreator<CollapsingAggregatePOP> cac = new CollaspsAggreBatchCreator();
  private BatchCreator<Union> uc = new UnionBatchCreator();
  private BatchCreator<UnionedScanSplitPOP> splitc = new UnionedScanSplitBatchCreator();
  private UnionedScanBatchCreator unionedScanc = new UnionedScanBatchCreator();
  private BatchCreator<Scan> sbc = new ScanBatchCreator();


  private AsyncExecutor executor = new AsyncExecutor();
  
  
  private AsyncImplCreator() {
  }

  public RootExec getRoot() {
    return root;
  }

  @Override
  public RecordBatch visitProject(Project op, FragmentContext context) throws ExecutionSetupException {
    return createBatchWithChildren(pc, op, context);
  }
  
  @Override
  public RecordBatch visitScan(Scan scan, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkNotNull(scan);
    Preconditions.checkNotNull(context);
    if(scan instanceof UnionedScanPOP){
      return createBatchWithChildren(unionedScanc, (UnionedScanPOP) scan, context); 
    }else{
      return createBatchWithChildren(sbc, scan, context);
    }
  }

  @Override
  public RecordBatch visitUnionedScanSplit(UnionedScanSplitPOP scan, FragmentContext context) throws ExecutionSetupException {
    return executor.createBatchWithoutChildrenRelay(splitc, scan, context, this);
  }

  @Override
  public RecordBatch visitOp(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    if (op instanceof SelectionVectorRemover){
      return createBatchWithChildren(svc, (SelectionVectorRemover) op, context);
    }
    return super.visitOp(op, context);
  }

  @Override
  public RecordBatch visitScreen(Screen op, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkArgument(root == null);
    root = executor.createRootBatchWithChildren(sc, op, context, this);
    return null;
  }

  @Override
  public RecordBatch visitFilter(Filter filter, FragmentContext context) throws ExecutionSetupException {
    return createBatchWithChildren(fc, filter, context);
  }


  @Override
  public RecordBatch visitSingleSender(SingleSender op, FragmentContext context) throws ExecutionSetupException {
    root = ssc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitRandomReceiver(RandomReceiver op, FragmentContext context) throws ExecutionSetupException {
    return createBatchWithChildren(rrc, op, context);
  }
  
  private List<RecordBatch> getChildren(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException {
    List<RecordBatch> children = Lists.newArrayList();
    for (PhysicalOperator child : op) {
      children.add(child.accept(this, context));
    }
    return children;
  }

  public static RootExec getExec(FragmentContext context, FragmentRoot root) throws ExecutionSetupException {
    AsyncImplCreator i = new AsyncImplCreator();
    root.accept(i, context);
    if (i.root == null)
      throw new ExecutionSetupException("The provided fragment did not have a root node that correctly created a RootExec value.");
    return i.getRoot();
  }

  @Override
  public RecordBatch visitUnion(Union union, FragmentContext value) throws ExecutionSetupException {
    return createBatchWithChildren(uc, union, value);
  }

  private <T extends PhysicalOperator> RecordBatch createBatchWithChildren(BatchCreator<T> creator, T operator, FragmentContext value) throws ExecutionSetupException {
    return executor.createBatchWithChildrenRelay(creator, operator, value, this);
  }

  @Override
  public RecordBatch visitCollapsingAggregate(CollapsingAggregatePOP op, FragmentContext value) throws ExecutionSetupException {
    return createBatchWithChildren(cac, op, value);
  }

  @Override
  public RecordBatch visitSegment(SegmentPOP op, FragmentContext value) throws ExecutionSetupException {
    return createBatchWithChildren(sgc, op, value);
  }

  @Override
  public RecordBatch visitJoin(JoinPOP op, FragmentContext value) throws ExecutionSetupException {
    return createBatchWithChildren(jc, op, value);
  }
  
}
