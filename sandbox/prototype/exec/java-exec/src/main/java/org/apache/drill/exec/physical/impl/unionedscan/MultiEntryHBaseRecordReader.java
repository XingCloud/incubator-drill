package org.apache.drill.exec.physical.impl.unionedscan;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;

public class MultiEntryHBaseRecordReader implements RecordReader {

  
  public MultiEntryHBaseRecordReader(FragmentContext context, HbaseScanPOP.HbaseScanEntry[] config) {
    //TODO
  }

    
  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    //TODO method implementation
  }

  @Override
  public int next() {
    return 0;  //TODO method implementation
  }

  @Override
  public void cleanup() {
    //TODO method implementation
  }
}
