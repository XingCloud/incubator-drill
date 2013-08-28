package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanBatch;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.Collections;
import java.util.List;

@JsonTypeName("unioned-scan")
public class UnionedScanPOP extends AbstractScan<HbaseScanPOP.HbaseScanEntry> {
  
  private UnionedScanBatch batch = null;
  
  @JsonCreator
  public UnionedScanPOP(@JsonProperty("entries") List<HbaseScanPOP.HbaseScanEntry> readEntries) {
    super(readEntries);
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
  }

  @Override
  public Scan<?> getSpecificScan(int minorFragmentId) {
    return this;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return this;
  }

  public UnionedScanBatch getBatch() {
    return batch;
  }

  public void setBatch(UnionedScanBatch batch) {
    this.batch = batch;
  }
}
