package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 10:25 AM
 */

@JsonTypeName("SegmentPOP")
public class SegmentPOP extends AbstractSingle {

    private LogicalExpression[] exprs ;

    public SegmentPOP(@JsonProperty("child") PhysicalOperator child, @JsonProperty("exprs") LogicalExpression[] exprs) {
        super(child);
        this.exprs = exprs;
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


    public LogicalExpression[] getExprs() {
        return exprs;
    }
}
