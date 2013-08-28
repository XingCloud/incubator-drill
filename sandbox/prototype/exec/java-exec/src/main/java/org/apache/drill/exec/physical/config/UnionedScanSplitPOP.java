package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.reentrant.ReentrantPhysicalOperator;

@JsonTypeName("unioned-scan-split")
public class UnionedScanSplitPOP  extends AbstractSingle implements ReentrantPhysicalOperator {

  private int[] entries;
  
  public UnionedScanSplitPOP(PhysicalOperator child, @JsonProperty("entries") int[] entries) {
    super(child);
    this.entries = entries;
  }

  @JsonProperty("entries")  
  public int[] getEntries() {
    return entries;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return this;
  }

  @Override
  public OperatorCost getCost() {
    return child.getCost();
  }
  
  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnionedScanSplit(this, value);
  }
}
