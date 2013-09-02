package org.apache.drill.exec.physical.impl;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/15/13
 * Time: 10:25 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestLocalQuery extends PopUnitTestBase {
    public Logger logger= org.slf4j.LoggerFactory.getLogger(TestLocalQuery.class);


    @Test
    public void physicalTest1() throws Exception {
        runNoExchangeFragment("/physical_test1.json");
    }
    @Test
    public void physicalTest2() throws Exception {
        runNoExchangeFragment("/physical_test2.json");
    }
    @Test
    public void physicalTest3() throws Exception {
        runNoExchangeFragment("/physical_test3.json");
    }
    @Test
    public void physicalTest4() throws Exception {
        runNoExchangeFragment("/physical_test4.json");
    }
    @Test
    public void physicalTest5() throws Exception {
        runNoExchangeFragment("/physical_test5.json");
    }
        private  void runNoExchangeFragment(String test) throws Exception {
        try(RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
            Drillbit bit = new Drillbit(CONFIG, serviceSet);
            DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile(test), Charsets.UTF_8));

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
                    for (VectorWrapper<?> value : batchLoader) {

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
                    for (VectorWrapper<?> value : batchLoader) {
                        if (first) {
                            first = false;
                        } else {
                            System.out.print("\t");
                        }
                        Object obj = value.getValueVector().getAccessor().getObject(i);
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

}
