package org.apache.drill.exec.expr;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;

public class ValueVectorWriteExpression implements LogicalExpression {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorWriteExpression.class);

  private final int fieldId;
  private final LogicalExpression child;
  
  public ValueVectorWriteExpression(int fieldId, LogicalExpression child){
    this.fieldId = fieldId;
    this.child = child;
  }
  
  public int getFieldId() {
    return fieldId;
  }

  @Override
  public MajorType getMajorType() {
    return Types.NULL;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }

  public LogicalExpression getChild() {
    return child;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ValueVectorWriteExpression that = (ValueVectorWriteExpression) o;

    if (fieldId != that.fieldId) return false;
    if (child != null ? !child.equals(that.child) : that.child != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = fieldId;
    result = 31 * result + (child != null ? child.hashCode() : 0);
    return result;
  }
}
