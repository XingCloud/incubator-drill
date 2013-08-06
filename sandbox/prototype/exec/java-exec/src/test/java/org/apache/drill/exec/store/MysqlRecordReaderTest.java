package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.config.MysqlScanPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.junit.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/5/13
 * Time: 4:17 PM
 */
public class MysqlRecordReaderTest {

  @Test
  public void testMysqlRecordReaderWithFilter() {
    DrillConfig c = DrillConfig.create();
    String project = "sof-dsk";
    String propertyName = "language";

    String tableName = project + "." + propertyName;
    List<NamedExpression> projections = Lists.newArrayList();
    FieldReference f = new FieldReference(propertyName, ExpressionPosition.UNKNOWN);

    String filter = "val = 'en'" ;

    NamedExpression uid = new NamedExpression(new FieldReference("uid", ExpressionPosition.UNKNOWN),
      new FieldReference("uid", ExpressionPosition.UNKNOWN));

    projections.add(uid);

    MysqlScanPOP.MysqlReadEntry readEntry = new MysqlScanPOP.MysqlReadEntry(tableName, filter, projections);

    MysqlRecordReader mysqlRecordReader = new MysqlRecordReader(null, readEntry);
    List<RecordReader> recordReaders = Lists.newArrayList();
    recordReaders.add(mysqlRecordReader);

    try {
      ScanBatch scanBatch = new ScanBatch(null, recordReaders.iterator());
      int recordSize = 0 ;
      while (scanBatch.next() != RecordBatch.IterOutcome.NONE) {
        recordSize +=   scanBatch.getRecordCount() ;
        System.out.println("Incoming record size :  " + recordSize);
      }

    } catch (Exception e) {

    }

  }

  @Test
  public void testMysqlRecordReaderNoFilter(){
    DrillConfig c = DrillConfig.create();
    String project = "sof-dsk";
    String propertyName = "language";

    String tableName = project + "." + propertyName;
    List<NamedExpression> projections = Lists.newArrayList();

    NamedExpression val = new NamedExpression(new FieldReference("val", ExpressionPosition.UNKNOWN),
      new FieldReference(propertyName, ExpressionPosition.UNKNOWN));

    NamedExpression uid = new NamedExpression(new FieldReference("uid", ExpressionPosition.UNKNOWN),
      new FieldReference("uid", ExpressionPosition.UNKNOWN));

    projections.add(uid);
    projections.add(val);

    MysqlScanPOP.MysqlReadEntry readEntry = new MysqlScanPOP.MysqlReadEntry(tableName, null, projections);

    MysqlRecordReader mysqlRecordReader = new MysqlRecordReader(null, readEntry);
    List<RecordReader> recordReaders = Lists.newArrayList();
    recordReaders.add(mysqlRecordReader);

    try {
      ScanBatch scanBatch = new ScanBatch(null, recordReaders.iterator());
      int recordCount = 0 ;
      while (scanBatch.next() != RecordBatch.IterOutcome.NONE) {
        recordCount += scanBatch.getRecordCount();
        System.out.println("Incoming record size :  " + recordCount);
      }

    } catch (Exception e) {
        e.printStackTrace();
    }
  }

}
