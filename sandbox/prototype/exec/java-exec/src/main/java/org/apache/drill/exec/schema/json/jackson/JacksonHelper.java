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

package org.apache.drill.exec.schema.json.jackson;

import java.io.IOException;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JacksonHelper {

    public static final MajorType STRING_TYPE = Types.optional(MinorType.VARCHAR);
    public static final MajorType BOOLEAN_TYPE = Types.optional(MinorType.BIT);
    public static final MajorType ARRAY_TYPE = Types.repeated(MinorType.LATE);
    public static final MajorType MAP_TYPE = Types.repeated(MinorType.MAP);
    public static final MajorType INT_TYPE = Types.optional(MinorType.INT);
    public static final MajorType FLOAT_TYPE = Types.optional(MinorType.FLOAT4);
    public static final MajorType NULL_TYPE = Types.optional(MinorType.LATE);

    public static MajorType getFieldType(JsonToken token) {
        switch(token) {
            case VALUE_STRING:
                return STRING_TYPE;
            case VALUE_FALSE:
                return BOOLEAN_TYPE;
            case VALUE_TRUE:
                return BOOLEAN_TYPE;
            case START_ARRAY:
                return ARRAY_TYPE;
            case START_OBJECT:
                return MAP_TYPE;
            case VALUE_NUMBER_INT:
                return INT_TYPE;
            case VALUE_NUMBER_FLOAT:
                return FLOAT_TYPE;
            case VALUE_NULL:
                return NULL_TYPE;
        }

        throw new UnsupportedOperationException("Unsupported Jackson type: " + token);
    }

    public static Object getValueFromFieldType(JsonParser parser, MinorType fieldType) throws IOException {
        switch (fieldType) {
            case INT:
                return parser.getIntValue();
            case VARCHAR:
                return parser.getValueAsString();
            case FLOAT4:
                return parser.getFloatValue();
            case BIT:
                return parser.getBooleanValue();
            case LATE:
                return null;
            default:
                throw new RuntimeException("Unexpected Field type to return value: " + fieldType.toString());
        }
    }
}
