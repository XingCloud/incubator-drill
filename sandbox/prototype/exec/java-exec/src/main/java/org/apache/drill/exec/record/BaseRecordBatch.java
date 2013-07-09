package org.apache.drill.exec.record;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.exec.record.vector.ValueVector;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/8/13
 * Time: 10:18 PM
 */
public abstract class BaseRecordBatch implements RecordBatch {

    protected RecordPointer record = new RecordPointer();
    protected IntObjectOpenHashMap<ValueVector<?>> fields = new IntObjectOpenHashMap<>();
    protected int recordCount;

    public abstract void setupEvals();


    @Override
    public int getRecordCount() {
        return recordCount;
    }

    @Override
    public RecordPointer getRecordPointer() {
        return record;
    }

    @Override
    public ValueVector getValueVector(int fieldId) throws InvalidValueAccessor {
        if (!fields.containsKey(fieldId)) throw new InvalidValueAccessor("Unknown accessor");
        return fields.lget();
    }

    @Override
    public <T extends ValueVector<T>> T getValueVector(int fieldId, Class<T> clazz) throws InvalidValueAccessor {
        return (T) getValueVector(fieldId);
    }

    @Override
    public WritableBatch getWritableBatch() {
        return WritableBatch.get(getRecordCount(), fields);
    }
}
