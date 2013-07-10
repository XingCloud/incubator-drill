package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/2/13
 * Time: 8:55 PM
 * To change this template use File | Settings | File Templates.
 */
public interface XAScanner {
    public boolean next(List<KeyValue> results) throws IOException;
    public void close() throws IOException;

}
