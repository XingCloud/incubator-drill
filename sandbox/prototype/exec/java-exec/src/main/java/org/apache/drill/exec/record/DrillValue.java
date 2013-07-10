package org.apache.drill.exec.record;

import org.apache.drill.exec.proto.SchemaDefProtos;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:49 PM
 */
public interface DrillValue {
    public abstract boolean isVector();
    public abstract SchemaDefProtos.MinorType getMinorType();
    public abstract boolean isNumeric();
}
