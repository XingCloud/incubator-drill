package org.apache.drill.exec.physical.impl.unionedscan;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;

public class MultiEntryHBaseRecordReader implements RecordReader {
  private HbaseScanPOP.HbaseScanEntry[] entries;
  private List<ValueVector[]> entryValueVectors;
  private OutputMutator outputMutator;
  private FragmentContext context;
  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  
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
