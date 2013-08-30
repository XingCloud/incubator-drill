package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
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

@JsonTypeName("segment")
public class SegmentPOP extends AbstractSingle {

    private NamedExpression[] exprs ;
    private FieldReference ref ;

    public SegmentPOP(@JsonProperty("child") PhysicalOperator child,
                      @JsonProperty("exprs") NamedExpression[] exprs,
                      @JsonProperty("ref") FieldReference ref) {
        super(child);
        this.exprs = exprs;
        this.ref = ref ;
    }

    @Override
    protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
        return new SegmentPOP(child,exprs,ref);
    }

    @Override
    public OperatorCost getCost() {
        return child.getCost();
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSegment(this,value);
    }


    public NamedExpression[] getExprs() {
        return exprs;
    }

    public FieldReference getRef() {
        return ref;
    }
}
