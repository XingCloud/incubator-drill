package org.apache.drill.exec.ref.rse;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData ;


/**
 * Created with IntelliJ IDEA.
 * User: hyx
 * Date: 13-3-12                                       ˆ
 * Time: 下午2:44
 * To change this template use File | Settings | File Templates.
 */
public class MysqlRecordReader implements RecordReader{


   // private static Logger LOG = LoggerFactory.getLogger(MysqlRecordReader.class);
    private String sql = null ;
    private ROP  parent ;
    private SchemaPath rootPath ;

    private ResultSet rs = null ;
    private ResultSetMetaData rsMetaData = null ;
    private int columnCount  ;
    private UnbackedRecord record = new UnbackedRecord() ;


    public MysqlRecordReader(String sql,ROP parent, SchemaPath rootPath)
    {
        this.sql = "Select register_time.uid FROM register_time WHERE register_time.val>=20130101000000 and register_time.val<20130102000000";//sql;
        this.parent = parent ;
        this.rootPath = rootPath ;

    }
    @Override
    public RecordIterator getIterator() {
        return new SqlRecordIter();
    }

    @Override
    public void setup() {


    }

    @Override
    public void cleanup() {

    }


    private boolean executeQuery()
    {
        try {
            Class.forName("com.mysql.jdbc.Driver") ;
            Connection conn = DriverManager.getConnection("jdbc:mysql://10.18.4.22:3306/fix_sof-dsk", "xadrill","123456");
            Statement stmt = conn.createStatement() ;
            rs = stmt.executeQuery(sql) ;
            rsMetaData = rs.getMetaData() ;
            columnCount = rsMetaData.getColumnCount() ;

        } catch (Exception e) {
            e.printStackTrace();
            return false ;
        }

        return true ;
    }

    private   DataValue convert(Object row)
    {

        return null ;
    }
    private  class  SqlRecordIter implements  RecordIterator
    {
        @Override
        public RecordPointer getRecordPointer() {
            return record;
        }

        @Override
        public NextOutcome next() {
            try {
                if (rs == null) {
                    executeQuery();
                }
                if (rs.next()) {
                    SimpleMapValue dataValue = new SimpleMapValue();
                    for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                        String columnName = rsMetaData.getColumnName(columnIndex);
                        DataValue dv = null;
                        // no need to transform
                        if ("uid".equals(columnName)) {
                            long uid = rs.getLong(columnIndex);
                            int innerUid = getInnerUidFromSamplingUid(uid);
                            dv = new ScalarValues.IntegerScalar(innerUid);
                            dataValue.setByName(columnName, dv);
                        } else {
                            columnName = rsMetaData.getTableName(columnIndex) ;
                            Object value = rs.getObject(columnIndex);

                            if (value instanceof String)
                                dv = new ScalarValues.StringScalar((String) value);
                            else
                                dv = new ScalarValues.LongScalar((Long) value);
                            dataValue.setByName(columnName, dv);
                        }
                    }
                    record.setClearAndSetRoot(rootPath, dataValue);
                    return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
                } else {
                    return NextOutcome.NONE_LEFT;
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RecordException("Failure while reading record", null, e);
            }

        }

        @Override
        public ROP getParent() {

            return parent;
        }
        public int getInnerUidFromSamplingUid(long suid) {
            return (int) (0xffffffffl & suid);
        }
    }

    public static void main(String[] args){
        SchemaPath schemaPath = new FieldReference("fix_sof-dsk");
        String sql = "select * from ref join register_time on ref.uid=register_time.uid limit 100; ";
        MysqlRecordReader  rr = new MysqlRecordReader(sql,null,schemaPath) ;

        System.out.println("start") ;
        RecordIterator recordIterator = rr.getIterator() ;

        RecordPointer record = null;
        while (recordIterator.next()  != RecordIterator.NextOutcome.NONE_LEFT)
        {
           record =  recordIterator.getRecordPointer() ;
            System.out.println("ok") ;
        }

        System.out.println("end") ;
    }
}
