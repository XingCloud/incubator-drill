package org.apache.drill.exec.physical.impl.eval.fn.agg;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionArguments;
import org.apache.drill.exec.physical.impl.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.DrillValue;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.record.values.ScalarValues;
import org.apache.drill.exec.record.vector.Fixed8;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/9/13
 * Time: 9:53 AM
 */

@FunctionEvaluator("count_distinct")
public class CountDistinctAggregator implements AggregatingEvaluator {


    private long l = 0;
    private BasicEvaluator child;
    private RecordPointer record;
    private Set<Object> duplicate = new HashSet<>();
    private Fixed8 value   ;

    public CountDistinctAggregator(RecordPointer record, FunctionArguments args) {
        this.child = args.getOnlyEvaluator();
        this.record = record;
        value = new Fixed8(null, BufferAllocator.getAllocator(null));
        value.allocateNew(1);
        value.setRecordCount(1);
    }

    @Override
    public void addBatch() {
        ValueVector v = (ValueVector) child.eval();
        Object o;
        for (int i = 0; i < v.getRecordCount(); i++) {
            o = v.getObject(i);
            if (!duplicate.contains(o)) {
                l++;
                duplicate.add(o);
            }
        }
    }

    @Override
    public DrillValue eval() {

        value.setBigInt(0, l);
        MaterializedField f = MaterializedField.create(new SchemaPath("count_distinct"), 0, 0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.UINT8));
        value.setField(f);
        l = 0;
        return value;
    }

    @Override
    public boolean isConstant() {
        return false;
    }
}
