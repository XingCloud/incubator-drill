package org.apache.drill.exec.store;

import com.xingcloud.hbase.util.HBaseUserUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.fn.BooleanFunctions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
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
public class TestHBaseRecordReader {
   @Test
   public void  readEventTable(){
       String startKey = System.getProperty("hbase.start");
       if(startKey==null)
           startKey="20130101visit.22find.hp.";
       System.out.println("startKey = " + startKey);
       String stopKey=System.getProperty("hbase.stop");
       if(stopKey==null)
           stopKey="20130101visit.";
       System.out.println("stopKey = "+stopKey);

       String eventTable=System.getProperty("hbase.table");
       if(eventTable==null)
           eventTable="sof-dsk_deu";
       System.out.println("eventTable "+eventTable);
       HbaseScanPOP.HbaseScanEntry eventEntry=getReadEntry(eventTable, startKey, stopKey, "event");

       HBaseRecordReader eventReader=new HBaseRecordReader(null,eventEntry);
       List<RecordReader> readerList = new ArrayList<RecordReader>();
       readerList.add(eventReader);
       readTables(readerList);


  }
  @Test
  public void readUserTable(){
      String startKey = System.getProperty("hbase.start");
      if(startKey==null)
      {
          startKey="\\x00\\x0320130619";
      }
      System.out.println("startKey = " + startKey);
      String stopKey=System.getProperty("hbase.stop");
      if(stopKey==null)
      {
          stopKey="\\x00\\x0320130619";
      }
      System.out.println("stopKey = "+stopKey);

      String usrTable=System.getProperty("hbase.table");
      if(usrTable==null)
          usrTable="property_age_index";
      System.out.println("userTable "+usrTable);
      HbaseScanPOP.HbaseScanEntry userEntry=getReadEntry(usrTable, startKey, stopKey,"user");
      HBaseRecordReader userReader=new HBaseRecordReader(null,userEntry);
      List<RecordReader> readerList = new ArrayList<RecordReader>();
      readerList.add(userReader);
      readTables(readerList);
  }


    private void readTables(List<RecordReader> readerList){
      Iterator<RecordReader> iter = readerList.iterator();
      long startTime = System.currentTimeMillis();

      int recordCount = 0;
      int count = 0;
      try {
          ScanBatch batch = new ScanBatch(null, iter);
          long ts1, ts2;
          ts1 = System.currentTimeMillis();
          while (batch.next() != RecordBatch.IterOutcome.NONE) {
              recordCount += batch.getRecordCount();
              ts2 = System.currentTimeMillis();
              System.out.println();
              System.out.println(recordCount + ": " + batch.getRecordCount() + " costs " + (ts2 - ts1) + "ms");
              ts1 = ts2;

              for (VectorWrapper<?> v : batch) {
                  System.out.print(v.getField().getName() + ": ");
                  ValueVector.Accessor accessor = v.getValueVector().getAccessor();
                  if (v instanceof VariableWidthVector) {
                      for (int i = 0; i < accessor.getValueCount(); i++) {
                          //System.out.print(Bytes.toString((byte[]) accessor.getObject(i)) + " ");
                          count++;
                      }
                  } else {
                      for (int i = 0; i < accessor.getValueCount(); i++) {
                          //System.out.print(accessor.getObject(i) + " ");
                          count++;
                      }
                  }
                  System.out.println();
              }


          }
      } catch (ExecutionSetupException e) {
          e.printStackTrace();
      }
      System.out.println("count " + count);
      System.out.println("Done , recordCount :" + recordCount + ", cost time " + (System.currentTimeMillis() - startTime) / 1000 + " seconds");

  }

   private HbaseScanPOP.HbaseScanEntry getReadEntry(String table, String startKey, String stopKey, String option){
       List<LogicalExpression> filters=new ArrayList<>();
       List<NamedExpression> projections=new ArrayList<>();
       HbaseScanPOP.HbaseScanEntry entry=null;
       switch (option){
           case "user":
                NamedExpression ue1=new NamedExpression(new SchemaPath("uid",ExpressionPosition.UNKNOWN),
                                                      new FieldReference("uid",ExpressionPosition.UNKNOWN));
                NamedExpression ue2=new NamedExpression(new SchemaPath("propnumber",ExpressionPosition.UNKNOWN),
                                                       new FieldReference("propId",ExpressionPosition.UNKNOWN));
                NamedExpression ue3=new NamedExpression(new SchemaPath("language",ExpressionPosition.UNKNOWN),
                       new FieldReference("val",ExpressionPosition.UNKNOWN));
                projections.add(ue1);
                projections.add(ue2);
                projections.add(ue3);
                entry=new HbaseScanPOP.HbaseScanEntry(table,startKey,stopKey,null,projections);
                return entry;
           case "event":
               FunctionDefinition funcDef=new BooleanFunctions().getFunctionDefintions()[3];
               List<LogicalExpression> funcArgs=new ArrayList<>();
               funcArgs.add(new SchemaPath("value",ExpressionPosition.UNKNOWN));
               funcArgs.add(new ValueExpressions.LongExpression(1000l,ExpressionPosition.UNKNOWN));
               FunctionCall func=new FunctionCall(funcDef,funcArgs,ExpressionPosition.UNKNOWN);
               filters.add(func);
               NamedExpression e1=new NamedExpression(new SchemaPath("uid",ExpressionPosition.UNKNOWN),new FieldReference("uid",ExpressionPosition.UNKNOWN));
               NamedExpression e2=new NamedExpression(new SchemaPath("event0",ExpressionPosition.UNKNOWN),new FieldReference("event0",ExpressionPosition.UNKNOWN));
               NamedExpression e3=new NamedExpression(new SchemaPath("value",ExpressionPosition.UNKNOWN),new FieldReference("value",ExpressionPosition.UNKNOWN));
               projections.add(e1);
               projections.add(e2);
               projections.add(e3);
               entry=new HbaseScanPOP.HbaseScanEntry(table,startKey,stopKey,null,projections);
               return entry;

    }
    return entry;
  }


}
