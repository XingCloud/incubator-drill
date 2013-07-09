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
import org.apache.drill.exec.record.values.NumericValue;

public class Fixed8 extends AbstractFixedValueVector<Fixed8> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Fixed8.class);

    public Fixed8(MaterializedField field, BufferAllocator allocator) {
        super(field, allocator, 8 * 8);
    }

    public final void setBigInt(int index, long value) {
        index *= 8;
        data.setLong(index, value);
    }

    public final long getBigInt(int index) {
        index *= 8;
        return data.getLong(index);
    }

    public final void setFloat8(int index, double value) {
        index *= 8;
        data.setDouble(index, value);
    }

    public final double getFloat8(int index) {
        index *= 8;
        return data.getDouble(index);
    }

    @Override
    public Object getObject(int index) {
        return getBigInt(index);
    }

    @Override
    public void setObject(int index, Object obj) {
        switch (field.getDef().getMajorType().getMinorType()) {
            case BIGINT:
            case UINT8:
                setBigInt(index, (Long) obj);
                break;
            case FLOAT8:
            case DECIMAL8:
                setFloat8(index, (Double) obj);
                break;
        }
    }

    @Override
    public DrillValue compareTo(DrillValue other) {
        Fixed1 fixed1 = new Fixed1(null, allocator);
        fixed1.allocateNew(this.getRecordCount());
        fixed1.setRecordCount(this.getRecordCount());
        switch (field.getDef().getMajorType().getMinorType()) {
            case BIGINT:
            case UINT8:
                compareAsInt(fixed1, other);
                break;
            case FLOAT8:
            case DECIMAL8:
                compareAsFloat(fixed1, other);
        }

        return fixed1;
    }

    private void compareAsFloat(Fixed1 fixed1, DrillValue other) {
        if (other.isVector()) {

        } else {
            byte b;
            double d;
            float o = ((NumericValue) other).getAsFloat();
            for (int i = 0; i < getRecordCount(); i++) {
                d = getFloat8(i);
                b = (byte) (d > o ? 1 : d == o ? 0 : -1);
                fixed1.setByte(i, b);
            }
        }
    }

    private void compareAsInt(Fixed1 fixed1, DrillValue other) {
        if (other.isVector()) {

        } else {
            byte b;
            long l;
            long o = ((NumericValue) other).getAsLong();
            for (int i = 0; i < getRecordCount(); i++) {
                l = getBigInt(i);
                b = (byte) (l > o ? 1 : l == o ? 0 : -1);
                fixed1.setByte(i, b);
            }
        }
    }
}