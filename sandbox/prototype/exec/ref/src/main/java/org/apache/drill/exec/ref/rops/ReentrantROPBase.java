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
package org.apache.drill.exec.ref.rops;

import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;

import java.util.*;

/**
 * Provides multiple RecordIterator for output.
 * Each output RecordIterator behaves same as those returned by ROPBase.getOutput()
 */
public abstract class ReentrantROPBase implements ROP, ReentrantROP{

  List<RecordIteratorHistory> buffer = new ArrayList<>();
  
  Set<RecordIterator> outputs = new HashSet<>();
  
  RecordIterator internal = null;
  
  public synchronized RecordIterator pickOutput() {
    if(internal == null){
      internal = getOutput();
    }
    ReentrantIterator output = new ReentrantIterator();
    outputs.add(output);
    return output;
  }

  public synchronized void discardOutput(RecordIterator iterator) {
    outputs.remove(iterator);
  }
  
  public synchronized void poll(){
    RecordIterator.NextOutcome outcome = internal.next();
    RecordPointer nextRecord = outcome == RecordIterator.NextOutcome.NONE_LEFT ? 
      null:internal.getRecordPointer().copy();
    buffer.add(new RecordIteratorHistory(outcome, nextRecord));
  }
  
  public class ReentrantIterator implements RecordIterator{
    
    ProxySimpleRecord record = new ProxySimpleRecord();
    int nextRecord = 0;
    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }

    @Override
    public NextOutcome next() {
      if(nextRecord>=buffer.size()){
        poll();
      }
      //would fail calling after NextOutcome.NONE_LEFT      
      record.setRecord(buffer.get(nextRecord).record); 
      return buffer.get(nextRecord++).outcome;
    }

    @Override
    public ROP getParent() {
      return ReentrantROPBase.this;
    }
  }
  
  public class RecordIteratorHistory{
    public RecordIterator.NextOutcome outcome;
    public RecordPointer record;

    public RecordIteratorHistory(RecordIterator.NextOutcome outcome, RecordPointer nextRecord) {
      this.outcome = outcome;
      this.record = nextRecord;
    }
  }
}
