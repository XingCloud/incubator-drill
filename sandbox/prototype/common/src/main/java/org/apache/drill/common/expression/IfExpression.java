/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.common.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;

public class IfExpression extends LogicalExpressionBase{
	static final Logger logger = LoggerFactory.getLogger(IfExpression.class);
	
	public final ImmutableList<IfCondition> conditions;
	public final LogicalExpression elseExpression;
	
	private IfExpression(ExpressionPosition pos, List<IfCondition> conditions, LogicalExpression elseExpression){
	  super(pos);
		this.conditions = ImmutableList.copyOf(conditions);
		this.elseExpression = elseExpression;
	}
	
	public static class IfCondition{
		public final LogicalExpression condition;
		public final LogicalExpression expression;
		
		public IfCondition(LogicalExpression condition, LogicalExpression expression) {
			//logger.debug("Generating IfCondition {}, {}", condition, expression);
			
			this.condition = condition;
			this.expression = expression;
		}

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      IfCondition that = (IfCondition) o;

      if (condition != null ? !condition.equals(that.condition) : that.condition != null) return false;
      if (expression != null ? !expression.equals(that.expression) : that.expression != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = condition != null ? condition.hashCode() : 0;
      result = 31 * result + (expression != null ? expression.hashCode() : 0);
      return result;
    }
    
  }
	
	@Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E{
    return visitor.visitIfExpression(this, value);
  }

  public static class Builder{
		List<IfCondition> conditions = new ArrayList<IfCondition>();
		private LogicalExpression elseExpression;
		private ExpressionPosition pos = ExpressionPosition.UNKNOWN;
		
		public Builder setPosition(ExpressionPosition pos){
		  this.pos = pos;
		  return this;
		}
		
		public Builder addCondition(IfCondition condition){
			conditions.add(condition);
            return this;
		}

    public Builder addConditions(Iterable<IfCondition> conditions) {
      for (IfCondition condition : conditions) {
        addCondition(condition);
      }
      return this;
    }
		
		public Builder setElse(LogicalExpression elseExpression) {
			this.elseExpression = elseExpression;
            return this;
		}
		
		public IfExpression build(){
		  Preconditions.checkNotNull(pos);
		  Preconditions.checkNotNull(conditions);
		  Preconditions.checkNotNull(conditions);
			return new IfExpression(pos, conditions, elseExpression);
		}
		
	}

  @Override
  public MajorType getMajorType() {
    return this.elseExpression.getMajorType();
  }

  public static Builder newBuilder(){
		return new Builder();
	}


  public Iterable<IfCondition> conditionIterable(){
    
    return ImmutableList.copyOf(conditions);
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    List<LogicalExpression> children = Lists.newLinkedList();
    
    for(IfCondition ic : conditions){
      children.add(ic.condition);
      children.add(ic.expression);
    }
    children.add(this.elseExpression);
    return children.iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IfExpression that = (IfExpression) o;

    if (conditions != null ? !conditions.equals(that.conditions) : that.conditions != null) return false;
    if (elseExpression != null ? !elseExpression.equals(that.elseExpression) : that.elseExpression != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = conditions != null ? conditions.hashCode() : 0;
    result = 31 * result + (elseExpression != null ? elseExpression.hashCode() : 0);
    return result;
  }
}
