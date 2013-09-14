package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.*;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.parser.DFARowKeyParser;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/31/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestParseRowkey {
    private static final Log LOG = LogFactory.getLog(TestParseRowkey.class);

    private Map<String, HBaseFieldInfo> rkFieldInfoMap=new HashMap<>();
    private List<KeyPart> primaryRowKeyParts;
    private String tableName;
    private DFARowKeyParser dfaRowKeyParser;

    private static final int BATCH_SIZE = 16 * 1024;


    @Before
    public void init() throws Exception {
        tableName="deu_age";
        List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, null);
        for (HBaseFieldInfo col : cols) {
            rkFieldInfoMap.put(col.fieldSchema.getName(), col);
        }
        primaryRowKeyParts = TableInfo.getRowKey(tableName, null);
        dfaRowKeyParser = new DFARowKeyParser(primaryRowKeyParts, rkFieldInfoMap);
    }




}
