package org.apache.drill.exec.physical.impl.unionedscan;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.TestQueryLogical;
import org.apache.drill.exec.client.TestQueryPhysical;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TestParseUnionedScanLP {
  private static DrillConfig CONFIG = null;
  static final Logger logger = LoggerFactory.getLogger(TestParseUnionedScanLP.class);
  @Test
  public void testLocalExec() throws Exception{
    CONFIG = DrillConfig.create();    
    try(RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet(); 
        Drillbit bit = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
    
    // run query.
    bit.run();
    client.connect();
    List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile("/unionedscan/test1.json"), Charsets.UTF_8));

    // look at records
    RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
    int recordCount = 0;
    for (QueryResultBatch batch : results) {
      if(!batch.hasData()) continue;
      boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
      boolean firstColumn = true;

      // print headers.
      if (schemaChanged) {
        System.out.println("\n\n========NEW SCHEMA=========\n\n");
        for (ValueVector value : batchLoader) {

          if (firstColumn) {
            firstColumn = false;
          } else {
            System.out.print("\t");
          }
          System.out.print(value.getField().getName());
          System.out.print("[");
          System.out.print(value.getField().getType().getMinorType());
          System.out.print("]");
        }
        System.out.println();
      }


      for (int i = 0; i < batchLoader.getRecordCount(); i++) {
        boolean first = true;
        recordCount++;
        for (ValueVector value : batchLoader) {
          if (first) {
            first = false;
          } else {
            System.out.print("\t");
          }
          Object obj = value.getAccessor().getObject(i);
          if(obj instanceof  byte[]) {
            obj = new String((byte[]) obj) ;
          }
          System.out.print(obj);
        }
        if(!first) System.out.println();
      }
    
  

    }
    logger.debug("Received results {}", results);
    //assertEquals(recordCount, 200);
    }    
  }
  
  @Test
  public void testRemoteExec() throws Exception{
    TestQueryPhysical.submitQuery("/unionedscan/test1.json", UserProtos.QueryType.LOGICAL);
  }
  
}
