package org.apache.drill.exec.physical.impl.unionedscan;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionedScanSplitPOP;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

public class UnionedScanSplitBatchCreator implements BatchCreator<UnionedScanSplitPOP> {
  @Override
  public RecordBatch getBatch(FragmentContext context, UnionedScanSplitPOP config, List<RecordBatch> children) throws ExecutionSetupException {
    UnionedScanBatch unionedScanBatch = (UnionedScanBatch) children.get(0);
    return unionedScanBatch.createSplitBatch(context, config);
  }
}
