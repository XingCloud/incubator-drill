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
package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import java.util.Iterator;

@JsonTypeName("filter")
public class Filter extends SingleInputOperator {
  private final LogicalExpression expr;

  @JsonCreator
  public Filter(@JsonProperty("expr") LogicalExpression expr) {
    this.expr = expr;
  }

  public LogicalExpression getExpr() {
    return expr;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitFilter(this, value);
  }

  @Override
  public void unregisterSubscriber(LogicalOperator operator) {
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return Iterators.singletonIterator(getInput());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    Filter that = (Filter) o;

    if (expr != null ? !expr.equals(that.expr) : that.expr != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (expr != null ? expr.hashCode() : 0);
    return result;
  }
}
