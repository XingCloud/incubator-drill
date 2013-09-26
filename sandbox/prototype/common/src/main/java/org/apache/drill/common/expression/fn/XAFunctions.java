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
package org.apache.drill.common.expression.fn;

import org.apache.drill.common.expression.ArgumentValidators.AnyTypeAllowed;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;

public class XAFunctions implements CallProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XAFunctions.class);

  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[]{
      FunctionDefinition.operator("min5", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "min5"),
      FunctionDefinition.operator("hour", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "hour"),
      FunctionDefinition
        .operator("div300", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "div300"),
      FunctionDefinition
        .operator("div3600", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "div3600"),
      FunctionDefinition
        .operator("sgmt300", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "sgmt300"),
      FunctionDefinition
        .operator("sgmt3600", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "sgmt3600"),
      FunctionDefinition.operator("date", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsAnySoft(), "date"),
      FunctionDefinition
        .operator("hid2inner", new AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "hid2inner")
    };
  }
}
