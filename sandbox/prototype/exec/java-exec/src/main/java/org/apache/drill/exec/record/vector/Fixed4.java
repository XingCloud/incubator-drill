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
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.values.NumericValue;

public class Fixed4 extends AbstractFixedValueVector<Fixed4> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Fixed4.class);

    public Fixed4(MaterializedField field, BufferAllocator allocator) {
        super(field, allocator, 4 * 8);
    }

    public final void setInt(int index, int value) {
        index *= 4;
        data.setInt(index, value);
    }

    @Override
    public void setObject(int index, Object obj) {
        if (field != null && field.getType().getMinorType() == SchemaDefProtos.MinorType.FLOAT4){
            setFloat4(index,((Float)obj));
        } else{
            setInt(index,(Integer)obj);
        }
    }

    public final int getInt(int index) {
        index *= 4;
        return data.getInt(index);
    }

    public final void setFloat4(int index, float value) {
        index *= 4;
        data.setFloat(index, value);
    }

    public final float getFloat4(int index) {
        index *= 4;
        return data.getFloat(index);
    }

    @Override
    public Object getObject(int index) {
        if (field != null && field.getType().getMinorType() == SchemaDefProtos.MinorType.FLOAT4) {
            return getFloat4(index);
        } else {
            return getInt(index);
        }
    }

    @Override
    public DrillValue compareTo(DrillValue other) {
        Fixed1 fixed1 = new Fixed1(null, allocator);
        fixed1.allocateNew(this.getRecordCount());
        fixed1.setRecordCount(this.getRecordCount());
        switch (field.getDef().getMajorType().getMinorType()) {
            case INT:
            case DATE:
            case UINT4:
                compareAsInt(fixed1, other);
                break;
            case FLOAT4:
            case DECIMAL4:
                compareAsFloat(fixed1, other);
        }

        return fixed1;
    }

    private void compareAsFloat(Fixed1 fixed1, DrillValue other) {
        if (other.isVector()) {

        } else {
            byte b;
            float f;
            float o = ((NumericValue) other).getAsFloat();
            for (int i = 0; i < getRecordCount(); i++) {
                f = getFloat4(i);
                b = (byte) (f > o ? 1 : f == o ? 0 : -1);
                fixed1.setByte(i, b);
            }
        }
    }

    private void compareAsInt(Fixed1 fixed1, DrillValue other) {
        if (other.isVector()) {

        } else {
            byte b;
            int l;
            long o = ((NumericValue) other).getAsLong();
            for (int i = 0; i < getRecordCount(); i++) {
                l = getInt(i);
                b = (byte) (l > o ? 1 : l == o ? 0 : -1);
                fixed1.setByte(i, b);
            }
        }
    }
}
