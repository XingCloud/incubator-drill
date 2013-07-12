package org.apache.drill.exec.physical.config;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/12/13
 * Time: 12:23 PM
 */
public class CollapsingAggregate extends AbstractSingle {

    public CollapsingAggregate(PhysicalOperator child) {
        super(child);
    }

    @Override
    protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
        return null;
    }

    @Override
    public OperatorCost getCost() {
        return null;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return null;
    }
}
