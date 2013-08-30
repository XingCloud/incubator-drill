package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.physical.impl.unionedscan.UnionedScanBatch;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("unioned-scan")
public class UnionedScanPOP extends AbstractGroupScan implements SubScan, GroupScan{
  
  private UnionedScanBatch batch = null;
  
  private List<HbaseScanPOP.HbaseScanEntry> entries;
  
  @JsonCreator
  public UnionedScanPOP(@JsonProperty("entries") List<HbaseScanPOP.HbaseScanEntry> readEntries) {
    this.entries = readEntries;
  }

  @JsonProperty("entries")
  public List<HbaseScanPOP.HbaseScanEntry> getEntries() {
    return entries;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return this;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return entries.size();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    return new Size(1,1);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
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

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return null;  //TODO method implementation
  }
}
