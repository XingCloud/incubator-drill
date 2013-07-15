package org.apache.drill.exec.physical.config;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/11/13
 * Time: 6:21 PM
 */

public class Aggregate extends AbstractSingle {

    private PhysicalOperator child;

    public Aggregate(PhysicalOperator child) {
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
