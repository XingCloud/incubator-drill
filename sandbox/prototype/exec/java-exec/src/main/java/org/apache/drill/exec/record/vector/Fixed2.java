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
import org.apache.drill.exec.record.MaterializedField;

public class Fixed2 extends AbstractFixedValueVector<Fixed2>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Fixed2.class);

  public Fixed2(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator, 2*8);
  }

  public final void setSmallInt(int index, short value){
    index*=2;
    data.setShort(index, value);
  }
  
  public final short getSmallInt(int index){
    index*=2;
    return data.getShort(index);
  }
  
  public final void setUInt2(int index, short value){
    setSmallInt(index, value);
  }
  
  public final short getUInt2(int index){
    return getSmallInt(index);
  }
  
  @Override
  public Object getObject(int index) {
    return getSmallInt(index);
  }

    @Override
    public void setObject(int index, Object obj) {

    }

}
