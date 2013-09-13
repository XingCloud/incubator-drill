package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.*;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.exec.util.parser.DFARowKeyParser;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

    //private int index=0;
    private Map<String, HBaseFieldInfo> rkFieldInfoMap=new HashMap<>();
    private Map<String, Object> rkObjectMap;
    private List<KeyPart> primaryRowKeyParts;
    private List<KeyPart>[] propRowKeyParts;
    private Map<String,HBaseFieldInfo>[] propRkFieldInfoMaps;
    private String[] propertyNames={"grade","identifier","language",
                                    "last_login_time","last_pay_time"};

    private void init() throws Exception {
        String tableName="testtable_100W_deu";
        String[]projections={"event0","uid","value"};
        List<String> options=Arrays.asList(projections);
        List<HBaseFieldInfo> cols = TableInfo.getCols(tableName, options);
        for (HBaseFieldInfo col : cols) {
            rkFieldInfoMap.put(col.fieldSchema.getName(), col);
        }
        primaryRowKeyParts=TableInfo.getRowKey(tableName,options);
        rkObjectMap=new HashMap<>();
    }

    private void initUserTable(String tableName) throws Exception{
        propRkFieldInfoMaps=new Map[propertyNames.length];
        propRowKeyParts=new List[propertyNames.length];
        for(int i=0;i<propertyNames.length;i++){
            //String[] projections={propertyNames[i],"uid"};
            String[] projections={"uid"};
            List<String> options=Arrays.asList(projections);
            List<HBaseFieldInfo> cols=TableInfo.getCols(tableName,options);
            propRkFieldInfoMaps[i]=new HashMap<>();
            for(HBaseFieldInfo col: cols){
                propRkFieldInfoMaps[i].put(col.fieldSchema.getName(),col);
            }
            propRowKeyParts[i]=TableInfo.getRowKey(tableName,options);
        }
        rkObjectMap=new HashMap<>();

    }










}
