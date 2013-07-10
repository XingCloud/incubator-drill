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
package org.apache.drill.exec.cache;

import com.google.protobuf.MessageLite;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

//import com.google.protobuf.Parser;

public abstract class ProtoBufWrap<T extends MessageLite> implements DataSerializable{
  //static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoBufWrap.class);
  
  T value;
  final Class<T> parser;
  
  public ProtoBufWrap(Class<T> parser){
    this(null, parser);
  }
  
  public ProtoBufWrap(T value, Class<T> parser){
    this.value = value;
    this.parser = parser;
  }
  
  @Override
  public void readData(DataInput arg0) throws IOException {
    int len = arg0.readShort();
    byte[] b = new byte[len];
    arg0.readFully(b);
      Method method= null;
      try {
          method = parser.getDeclaredMethod("parseFrom",new Class[]{byte[].class});
      } catch (NoSuchMethodException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      try {
          this.value=(T)method.invoke(null,b);
      } catch (IllegalAccessException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (InvocationTargetException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
  }

  @Override
  public void writeData(DataOutput arg0) throws IOException {
    byte[] b = value.toByteArray();
    if (b.length > Short.MAX_VALUE) throw new IOException("Unexpectedly long value.");
    arg0.writeShort(b.length);
    arg0.write(b);
  }

  protected T get() {
    return value;
  }

  protected void set(T value) {
    this.value = value;
  }

}
