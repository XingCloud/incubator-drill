package org.apache.drill.exec.logical;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/12/13
 * Time: 4:49 PM
 */
public class TestLogicalRun extends PopUnitTestBase {

    @Test
    public void runLogicalPlan() throws Exception {

        try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
             Drillbit bit = new Drillbit(CONFIG, serviceSet);
             DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

            // run query.
            bit.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL, Files
                    .toString(FileUtils.getResourceAsFile("/physical_test2.json"), Charsets.UTF_8));

            // look at records
            RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
            int recordCount = 0;
            for (QueryResultBatch batch : results) {
                if (!batch.hasData())
                    continue;
                boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
                boolean firstColumn = true;

                // print headers.
                if (schemaChanged) {
                    System.out.println("\n\n========NEW SCHEMA=========\n\n");
                    for (IntObjectCursor<ValueVector<?>> v : batchLoader) {

                        if (firstColumn) {
                            firstColumn = false;
                        } else {
                            System.out.print("\t");
                        }
                        System.out.print(v.value.getField().getName());
                        System.out.print("[");
                        System.out.print(v.value.getField().getType().getMinorType());
                        System.out.print("]");
                    }
                    System.out.println();
                }

                for (int i = 0; i < batchLoader.getRecordCount(); i++) {
                    boolean first = true;
                    recordCount++;
                    for (IntObjectCursor<ValueVector<?>> v : batchLoader) {
                        if (first) {
                            first = false;
                        } else {
                            System.out.print("\t");
                        }
                        System.out.print(v.value.getObject(i));
                    }
                    if(!first)
                        System.out.println();

                }
                //assertEquals(recordCount, 200);


            }

        }

    }

}
