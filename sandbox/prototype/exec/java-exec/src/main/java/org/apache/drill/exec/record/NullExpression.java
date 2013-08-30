package org.apache.drill.exec.record;

import java.util.Iterator;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

import com.google.common.collect.Iterators;

public class NullExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullExpression.class);

  public static final NullExpression INSTANCE = new NullExpression();
  
  final MajorType t = Types.optional(MinorType.NULL);
  
  @Override
  public MajorType getMajorType() {
    return t;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }
  
  public boolean equals(Object o) {
    if(o != null){
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return t != null ? t.hashCode() : 0;
  }
}
