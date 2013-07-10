package org.apache.drill.exec.record;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.values.DataValue;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/2/13
 * Time: 10:38 PM
 * To change this template use File | Settings | File Templates.
 */
public interface RecordPointer {
    public DataValue getField(SchemaPath field);
    public void addField(SchemaPath field, DataValue value);
    public void addField(PathSegment segment, DataValue value);
    public void removeField(SchemaPath segment);
    public void write(DataWriter writer) throws IOException;
    public RecordPointer copy();
    public void copyFrom(RecordPointer r);

}
