package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

/**
 * User: Z J Wu Date: 13-7-12 Time: 下午2:58 Package: org.apache.drill.exec.physical.config
 */
public class PhysicalCollapsingAggregate extends AbstractSingle {

  private final FieldReference within;
  private final FieldReference target;
  private final FieldReference[] carryovers;
  private final NamedExpression[] aggregations;

  public PhysicalCollapsingAggregate(@JsonProperty("child") PhysicalOperator child,
                                     @JsonProperty("within") FieldReference within,
                                     @JsonProperty("target") FieldReference target,
                                     @JsonProperty("carryovers") FieldReference[] carryovers,
                                     @JsonProperty("aggregations") NamedExpression[] aggregations) {
    super(child);
    this.within = within;
    this.target = target;
    this.carryovers = carryovers;
    this.aggregations = aggregations;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new PhysicalCollapsingAggregate(child, within, target, carryovers, aggregations);
  }

  @Override
  public OperatorCost getCost() {
    return child.getCost();
  }

  @Override

  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

  public FieldReference getWithin() {
    return within;
  }

  public FieldReference getTarget() {
    return target;
  }

  public FieldReference[] getCarryovers() {
    return carryovers;
  }

  public NamedExpression[] getAggregations() {
    return aggregations;
  }
}
