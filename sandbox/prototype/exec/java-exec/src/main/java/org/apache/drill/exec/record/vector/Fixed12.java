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

public class Fixed12 extends AbstractFixedValueVector<Fixed12>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Fixed12.class);

  public Fixed12(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator, 12*8);
  }

  
  @Override
  public Object getObject(int index) {
    return null;
  }

    @Override
    public void setObject(int index, Object obj) {

    }

    @Override
    public DrillValue compareTo(DrillValue other) {
        return null;
    }
}
