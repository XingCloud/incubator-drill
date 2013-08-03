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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.common.logical.ValidationError;
import org.apache.drill.common.util.PathScanner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class LogicalOperatorBase implements LogicalOperator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogicalOperatorBase.class);

  private List<LogicalOperator> children = new ArrayList<LogicalOperator>();
  private String memo;

  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public void setupAndValidate(List<LogicalOperator> operators, Collection<ValidationError> errors) {
    // TODO: remove this and implement individually.
  }

  @Override
  public void accept(GraphVisitor<LogicalOperator> visitor) {
    if (visitor.enter(this)) {
      for (LogicalOperator o : children) {
        o.accept(visitor);
      }
    }
    visitor.leave(this);
  }

  @Override
  public void registerAsSubscriber(LogicalOperator operator) {
    if (operator == null)
      throw new IllegalArgumentException("You attempted to register a null operators.");
    children.add(operator);
  }

  @Override
  public void unregisterSubscriber(LogicalOperator operator) {
    if (operator == null) {
      throw new IllegalArgumentException("You attempted to unregister a null operators.");
    }
    children.remove(operator);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " [memo=" + memo + "]";
  }

  @JsonInclude(Include.NON_EMPTY) @JsonProperty("memo")
  public String getMemo() {
    return memo;
  }

  public void setMemo(String memo) {
    this.memo = memo;
  }

  public synchronized static Class<?>[] getSubTypes(DrillConfig config) {
    Class<?>[] ops = PathScanner.scanForImplementationsArr(LogicalOperator.class, config
      .getStringList(CommonConstants.LOGICAL_OPERATOR_SCAN_PACKAGES));
    logger.debug("Adding Logical Operator sub types: {}", ((Object) ops));
    return ops;
  }

  @JsonIgnore
  public List<LogicalOperator> getAllSubscribers() {
    return new ArrayList<LogicalOperator>(this.children);
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LogicalOperatorBase that = (LogicalOperatorBase) o;

    if (children != null ? !children.equals(that.children) : that.children != null) return false;

    return true;
  }
}
