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
package org.apache.drill.exec.physical.base;

import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.reentrant.ReentrantDelegate;
import org.apache.drill.exec.physical.base.reentrant.ReentrantOperator;
import org.apache.drill.exec.physical.base.reentrant.ReentrantPhysicalOperator;
import org.apache.drill.exec.record.buffered.IterationBuffer;

public abstract class AbstractBase implements PhysicalOperator, ReentrantPhysicalOperator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBase.class);

  private final ReentrantOperator reentrantDelegate = new ReentrantDelegate();

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    if(this.iterator() == null) throw new IllegalArgumentException("Null iterator for pop." + this);
    for(PhysicalOperator o : this){
      o.accept(visitor);
    }
    visitor.leave(this);
  }
  
  @Override
  public boolean isExecutable() {
    return true;
  }
  
  @Override
  public IterationBuffer getIterationBuffer() {
    return reentrantDelegate.getIterationBuffer();
  }

  @Override
  public void initIterationBuffer(IterationBuffer buffer) {
    reentrantDelegate.initIterationBuffer(buffer);
  }  
  
}
