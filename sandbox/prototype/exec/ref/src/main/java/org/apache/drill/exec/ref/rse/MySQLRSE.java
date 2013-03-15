package org.apache.drill.exec.ref.rse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ref.rops.ROP;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 3/11/13
 * Time: 6:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLRSE extends RSEBase {

    public MySQLRSE(MySQLRSEConfig engineConfig, DrillConfig config){

    }

    @JsonTypeName("mysql")
    public static class MySQLRSEConfig extends StorageEngineConfigBase {
        @JsonCreator
        public MySQLRSEConfig(@JsonProperty("name") String name) {
            super(name);
        }
    }

    public static class MySQLInputConfig implements ReadEntry{
        public String sql;
        @JsonIgnore
        public SchemaPath rootPath;
    }

    @Override
    public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
        MySQLInputConfig mySQLInputConfig = scan.getSelection().getWith(MySQLInputConfig.class);
        mySQLInputConfig.rootPath = scan.getOutputReference();
        return Collections.singleton((ReadEntry) mySQLInputConfig);
    }

    @Override
    public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
        MySQLInputConfig e = getReadEntry(MySQLInputConfig.class, readEntry);
        return new MysqlRecordReader(e.sql, parentROP, e.rootPath);
        //return  new MysqlRecordReader("select * from register_time where register_time.val>=20130101000000 and register_time.val<20130102000000", parentROP, e.rootPath);
    }
}
