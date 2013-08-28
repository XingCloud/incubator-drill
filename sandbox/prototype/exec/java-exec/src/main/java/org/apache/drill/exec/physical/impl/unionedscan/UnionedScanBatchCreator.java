package org.apache.drill.exec.physical.impl.unionedscan;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionedScanPOP;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * 生成针对每个UnionedScan，生成一个UnionedScanBatch对象，用来进一步生成UnionedScanSplitBatch
 */
public class UnionedScanBatchCreator implements BatchCreator<UnionedScanPOP> {

  @Override
  public RecordBatch getBatch(FragmentContext context, UnionedScanPOP config, List<RecordBatch> children) throws ExecutionSetupException {
    if(config.getBatch()==null){
      config.setBatch(new UnionedScanBatch(context, config.getReadEntries()));
    }
    return config.getBatch();
  }
}
