package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.config.MysqlScanPOP;
import org.apache.drill.exec.physical.config.UserScanPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

/**
 * Author: liqiang
 * Date: 15-1-4
 * Time: 下午4:16
 */
public class TestUserRecordReader {

    public static void main(String[] args){
        String project = args[0];
        String propertyName = args[1];
        int pid = Integer.parseInt(args[2]);
        int aid = Integer.parseInt(args[3]);
        String filter = args[4];

        DrillConfig c = DrillConfig.create();

        String tableName = project + "." + propertyName + "." + pid + "." + aid;
        List<NamedExpression> projections = Lists.newArrayList();
        FieldReference f = new FieldReference(propertyName, ExpressionPosition.UNKNOWN);


        NamedExpression uid = new NamedExpression(new FieldReference("uid", ExpressionPosition.UNKNOWN),
                new FieldReference("uid", ExpressionPosition.UNKNOWN));

        projections.add(uid);

        UserScanPOP.UserReadEntry readEntry = new UserScanPOP.UserReadEntry(tableName, filter, projections);

        UserRecordReader userRecordReader = new UserRecordReader(null, readEntry);
        List<RecordReader> recordReaders = Lists.newArrayList();
        recordReaders.add(userRecordReader);

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
}
