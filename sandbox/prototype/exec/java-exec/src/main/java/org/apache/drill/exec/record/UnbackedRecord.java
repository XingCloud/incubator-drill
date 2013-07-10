package org.apache.drill.exec.record;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.ContainerValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.drill.exec.ref.values.ValueUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/2/13
 * Time: 10:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class UnbackedRecord implements RecordPointer {

    private DataValue root = new SimpleMapValue();

    public DataValue getField(SchemaPath field) {
        return root.getValue(field.getRootSegment());
    }

    public void addField(SchemaPath field, DataValue value) {
        addField(field.getRootSegment(), value);
    }

    @Override
    public void addField(PathSegment segment, DataValue value) {
        root.addValue(segment, value);
    }

    @Override
    public void removeField(SchemaPath field) {
        root.removeValue(field.getRootSegment());
    }

    @Override
    public void write(DataWriter writer) throws IOException {
        writer.startRecord();
        root.write(writer);
        writer.endRecord();
    }

    public void merge(DataValue v) {
        if (v instanceof ContainerValue) {
            this.root = ValueUtils.getMergedDataValue(ValueExpressions.CollisionBehavior.MERGE_OVERRIDE, root, v);
        } else {
            this.root = v;
        }
    }

    public void merge(RecordPointer pointer) {
        if (pointer instanceof UnbackedRecord) {
            merge(UnbackedRecord.class.cast(pointer).root);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unable to merge from a record of type %s to an UnbackedRecord.", pointer.getClass().getCanonicalName())
            );
        }
    }

    @Override
    public RecordPointer copy() {
        // TODO: Make a deep copy.
        UnbackedRecord r = new UnbackedRecord();
        r.root = this.root;
        return r;
    }

    public void clear() {
        root = new SimpleMapValue();
    }

    public void setClearAndSetRoot(SchemaPath path, DataValue v) {
        root = new SimpleMapValue();
        root.addValue(path.getRootSegment(), v);
    }

    @Override
    public void copyFrom(RecordPointer r) {
        if (r instanceof UnbackedRecord) {
            this.root = ((UnbackedRecord) r).root.copy();
        } else {
            throw new UnsupportedOperationException(String.format("Unable to copy from a record of type %s to an UnbackedRecord.", r.getClass().getCanonicalName()));
        }
    }

    @Override
    public String toString() {
        return "UnbackedRecord [root=" + root + "]";
    }


}