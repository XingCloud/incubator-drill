package org.apache.drill.exec.record;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/7/13
 * Time: 3:49 PM
 */
public interface DrillValue {
    public abstract boolean isVector();
    public abstract DrillValue compareTo(DrillValue other);
}
