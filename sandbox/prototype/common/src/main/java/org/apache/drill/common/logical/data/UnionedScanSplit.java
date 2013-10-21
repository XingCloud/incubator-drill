package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.Iterator;

@JsonTypeName("unioned-scan-split")
public class UnionedScanSplit extends SingleInputOperator {

  //表示这个split依次需要UnionedScan的哪几个entry的扫描结果作为输出。
  //entry的顺序即输出的顺序。
  private int[] entries;

  @JsonCreator
    public UnionedScanSplit(@JsonProperty("entries") int[] entries) {
    this.entries = entries;
  }

  @JsonProperty("entries")
  public int[] getEntries(){
    return entries;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitUnionedScanSplit(this, value);
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return Iterators.singletonIterator(getInput());
  }

  public void setEntries(int[] entries) {
    this.entries = entries;
  }
}
