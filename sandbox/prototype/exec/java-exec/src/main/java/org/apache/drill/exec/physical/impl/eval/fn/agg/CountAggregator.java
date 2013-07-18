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
import org.apache.drill.exec.record.vector.Fixed8;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:49 AM
 */

@FunctionEvaluator("count")
public class CountAggregator implements AggregatingEvaluator{

    private long l = 0l;
    private BasicEvaluator child ;
    private RecordPointer record ;


    public CountAggregator(RecordPointer record , FunctionArguments args) {
        this.record = record;
         child = args.getOnlyEvaluator();
    }

    @Override
    public void addBatch() {

        l += record.getFields().get(record.getFieldsInfo().get(0).getFieldId()).getRecordCount() ;
    }

    @Override
    public Fixed8 eval() {
        Fixed8 value = new Fixed8(null, BufferAllocator.getAllocator(null));
        value.allocateNew(1);
        value.setRecordCount(1);
        value.setBigInt(0,l);
        l = 0;
        MaterializedField f = MaterializedField.create(new SchemaPath("count"),0,0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.BIGINT));
        value.setField(f);
        return value;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
