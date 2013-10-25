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
package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.concurrent.atomic.AtomicInteger;

public class DirectBufferAllocator extends BufferAllocator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectBufferAllocator.class);

  private final PooledByteBufAllocator buffer = new PooledByteBufAllocator(true);
  private AtomicInteger allocateSize  = new AtomicInteger(0) ;

  @Override
  public ByteBuf buffer(int size) {
    ByteBuf buf = buffer.directBuffer(size) ;
    allocateSize.addAndGet(buf.capacity()) ;
    return buf;
  }
  
  @Override
  protected boolean pre(int bytes) {
    // TODO: check allocation
    return true;
  }

  @Override
  public long free(ByteBuf byteBuf) {
    return allocateSize.addAndGet(-byteBuf.capacity()) ;
  }

  @Override
  public long getAllocatedMemory() {
    return allocateSize.get();
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return buffer;
  }

  

  @Override
  public BufferAllocator getChildAllocator(long initialReservation, long maximumReservation) {
    //TODO: Add child account buffer.
    return this;
  }

  @Override
  public void close() {
    if(allocateSize.get() != 0){
      logger.debug("Memory leak exists . " + allocateSize + " allocated bytes not released .");
    }
    // TODO: collect all buffers and release them away using a weak hashmap so we don't impact pool work
  }
  
}
