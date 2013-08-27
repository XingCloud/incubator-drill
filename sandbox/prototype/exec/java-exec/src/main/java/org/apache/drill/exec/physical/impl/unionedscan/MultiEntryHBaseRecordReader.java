package org.apache.drill.exec.physical.impl.unionedscan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;

import java.util.List;
import java.util.Map;

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
