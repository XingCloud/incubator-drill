package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;

import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA. User: witwolf Date: 7/16/13 Time: 10:52 AM
 */

@JsonTypeName("join")
public class PhysicalJoin extends AbstractBase {

  private PhysicalOperator left;
  private PhysicalOperator right;
  private JoinCondition conditoin;

  public PhysicalJoin(@JsonProperty("leftChild") PhysicalOperator left,
                      @JsonProperty("rightChild") PhysicalOperator right,
                      @JsonProperty("conditoin") JoinCondition conditoin) {
    this.left = left;
    this.right = right;
    this.conditoin = conditoin;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return null;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.forArray(left, right);
  }
}
