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
package org.apache.drill.exec.work.foreman;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.slf4j.Logger;

import java.util.UUID;


public class ErrorHelper {
  
  public static DrillPBError logAndConvertError(DrillbitEndpoint endpoint, String message, Throwable t, Logger logger){
    String id = UUID.randomUUID().toString();
    DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setEndpoint(endpoint);
    builder.setErrorId(id);
    if(message != null){
      builder.setMessage(message);  
    }else{
      builder.setMessage(t.getMessage());
    }
    builder.setErrorType(0);
    
    // record the error to the log for later reference.
    //logger.error("Error {}: {}", id, message, t);
    
    
    return builder.build();
  }
}
