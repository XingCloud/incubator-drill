package org.apache.drill.exec.store;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.fn.BooleanFunctions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.config.HbaseAbstractScanPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/24/13
 * Time: 2:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestHBaseAbstractRecordReader {
   @Test
   public void  testAbstractRecordReader(){
       byte[] srk= Bytes.toBytes("20121201");
       byte[] enk= Bytes.toBytes("20121202");
       String table="testtable100W_deu";
       List<LogicalExpression> filters=new ArrayList<>();
       FunctionDefinition funcDef=new BooleanFunctions().getFunctionDefintions()[3];
       List<LogicalExpression> funcArgs=new ArrayList<>();
       funcArgs.add(new SchemaPath("value",ExpressionPosition.UNKNOWN));
       funcArgs.add(new ValueExpressions.LongExpression(1000l,ExpressionPosition.UNKNOWN));
       FunctionCall func=new FunctionCall(funcDef,funcArgs,ExpressionPosition.UNKNOWN);
       filters.add(func);
       NamedExpression e1=new NamedExpression(new SchemaPath("uid",ExpressionPosition.UNKNOWN),new FieldReference("uid",ExpressionPosition.UNKNOWN));
       NamedExpression e2=new NamedExpression(new SchemaPath("event0",ExpressionPosition.UNKNOWN),new FieldReference("event0",ExpressionPosition.UNKNOWN));
       NamedExpression e3=new NamedExpression(new SchemaPath("value",ExpressionPosition.UNKNOWN),new FieldReference("value",ExpressionPosition.UNKNOWN));
       List<NamedExpression> projections=new ArrayList<>();
       projections.add(e1);
       projections.add(e2);
       projections.add(e3);
       HbaseAbstractScanPOP.HbaseAbstractScanEntry entry=
               new HbaseAbstractScanPOP.HbaseAbstractScanEntry(table,srk,enk,filters,projections);
       HBaseAbstractRecordReader reader=new HBaseAbstractRecordReader(null,entry);
       List<RecordReader> readerList = new ArrayList<RecordReader>();
       readerList.add(reader);
       Iterator<RecordReader> iter = readerList.iterator();
       long startTime = System.currentTimeMillis();

       int recordCount=0;
       int count=0;
       try {
           ScanBatch batch = new ScanBatch(null, iter);
           long ts1,ts2;
           ts1=System.currentTimeMillis();
           while (batch.next() != RecordBatch.IterOutcome.NONE) {
               recordCount += batch.getRecordCount();
               ts2=System.currentTimeMillis();
               System.out.println();
               System.out.println(recordCount+": "+batch.getRecordCount()+" costs "+(ts2-ts1)+"ms");
               ts1=ts2;

             /*
               for (MaterializedField f : batch.getSchema()) {
                   ValueVector v = batch.getValueVector(f.getFieldId());
                   System.out.print(f.getName() + ":");
                   if (v instanceof VarLen4) {
                       for (int i = 0; i < v.getRecordCount(); i++) {
                           System.out.print(new String((byte[]) v.getObject(i)) + " ");
                           count++;
                       }
                   } else {
                       for (int i = 0; i < v.getRecordCount(); i++) {
                           System.out.print(v.getObject(i) + " ");
                       }
                   }
                   System.out.println();
               }  */
           }
       } catch (ExecutionSetupException e) {
           e.printStackTrace();
       }
       System.out.println("count "+count);
       System.out.println("Done , recordCount :" +  recordCount + ", cost time " + (System.currentTimeMillis() - startTime)/1000 + " seconds");

   }

}
