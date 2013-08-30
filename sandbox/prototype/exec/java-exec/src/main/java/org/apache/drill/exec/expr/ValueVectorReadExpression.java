package org.apache.drill.exec.expr;

import java.util.Iterator;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.collect.Iterators;

public class ValueVectorReadExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorReadExpression.class);

  private final MajorType type;
  private final TypedFieldId fieldId;
  private final boolean superReader;
  
  
  public ValueVectorReadExpression(TypedFieldId tfId){
    this.type = tfId.getType();
    this.fieldId = tfId;
    this.superReader = tfId.isHyperReader();
  }
  
  public TypedFieldId getTypedFieldId(){
    return fieldId;
  }
  
  public boolean isSuperReader(){
    return superReader;
  }
  @Override
  public MajorType getMajorType() {
    return type;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  public TypedFieldId getFieldId() {
    return fieldId;
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ValueVectorReadExpression that = (ValueVectorReadExpression) o;

    if (superReader != that.superReader) return false;
    if (fieldId != null ? !fieldId.equals(that.fieldId) : that.fieldId != null) return false;
    if (type != null ? !type.equals(that.type) : that.type != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (fieldId != null ? fieldId.hashCode() : 0);
    result = 31 * result + (superReader ? 1 : 0);
    return result;
  }
}
