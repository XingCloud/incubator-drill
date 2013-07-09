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
package org.apache.drill.exec.record.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.MaterializedField;

public class VarLen2 extends VariableVector<VarLen2, Fixed2>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLen2.class);

  public VarLen2(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator, 2);
  }

  @Override
  protected Fixed2 getNewLengthVector(BufferAllocator allocator) {
    return new Fixed2(null, allocator);
  }

    @Override
    public DrillValue compareTo(DrillValue other) {
        return null;
    }

    @Override
    public void setObject(int index, Object obj) {

    }
}
