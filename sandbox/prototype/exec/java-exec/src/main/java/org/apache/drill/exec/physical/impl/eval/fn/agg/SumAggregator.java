package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.record.values.ScalarValues;
import org.apache.drill.exec.record.vector.Fixed4;
import org.apache.drill.exec.record.vector.Fixed8;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:55 AM
 */

@FunctionEvaluator("sum")
public class SumAggregator implements AggregatingEvaluator {

    private BasicEvaluator child;
    private long l = 0;
    private double d = 0;
    private boolean integer = true;
    private RecordPointer record;

    public SumAggregator(RecordPointer record, FunctionArguments args) {
        this.record = record;
        child = args.getOnlyEvaluator();
    }

    @Override
    public void addBatch() {

       // ValueVector vv = record.getFields().values().iterator().next().value;

        ValueVector vv = getSumColumn();
        int recordCount = vv.getRecordCount();
        int i;
        long sum = 0;
        switch (vv.getField().getType().getMinorType()) {
            case BIGINT:
            case UINT8:
                Fixed8 longs = (Fixed8) vv;
                for (i = 0; i < recordCount; i++) {
                    sum += longs.getBigInt(i);
                }
                l += sum;
                d += sum;
                break;
            case FLOAT8:
            case DECIMAL8:
                integer = false;
                Fixed8 doubles = (Fixed8) vv;
                for (i = 0; i < recordCount; i++) {
                    d += doubles.getFloat8(i);
                }
                break;
        }

    }


    @Override
    public DrillValue eval() {
        Fixed8 value = new Fixed8(null, BufferAllocator.getAllocator(null));
        value.allocateNew(1);
        value.setRecordCount(1);
        if (integer) {
            value.setBigInt(0, l);
        } else {
            value.setFloat8(0, d);
        }
        l = 0;
        d = 0;
        MaterializedField f = MaterializedField.create(new SchemaPath("sum"), 0, 0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.BIGINT));
        value.setField(f);
        return value;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    private ValueVector getSumColumn(){
        String columnName =  ((ScalarValues.StringScalar) child.eval() ).getString().toString() ;
        SchemaPath schemaPath = new SchemaPath(columnName);
        for(MaterializedField f : record.getFieldsInfo()){
            if(f.matches(schemaPath)){
                return record.getFields().get(f.getFieldId());
            }
        }
        return null;
    }
}
