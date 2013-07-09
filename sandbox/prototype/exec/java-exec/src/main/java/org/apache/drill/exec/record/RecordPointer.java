package org.apache.drill.exec.record;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.drill.exec.record.vector.ValueVector;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 4:00 PM
 */
public class RecordPointer {
    private List<MaterializedField> fieldsInfo;
    private IntObjectOpenHashMap<ValueVector<?>> fields;


    public RecordPointer() {

    }

    public void set(List<MaterializedField> fieldsInfo,IntObjectOpenHashMap<ValueVector<?>> fields){
        this.fieldsInfo = fieldsInfo;
        this.fields = fields;
    }

    public RecordPointer(List<MaterializedField> fieldsInfo, IntObjectOpenHashMap<ValueVector<?>> fields) {
        this.fieldsInfo = fieldsInfo;
        this.fields = fields;
    }

    public void setFieldsInfo(List<MaterializedField> fieldsInfo) {
        this.fieldsInfo = fieldsInfo;
    }

    public void setFields(IntObjectOpenHashMap<ValueVector<?>> fields) {
        this.fields = fields;
    }

    public List<MaterializedField> getFieldsInfo() {
        return fieldsInfo;
    }

    public IntObjectOpenHashMap<ValueVector<?>> getFields() {
        return fields;
    }
}
