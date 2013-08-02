package org.apache.drill.outer.manual;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;

import java.util.UUID;

/**
 * User: Z J Wu Date: 13-7-9 Time: 上午10:24 Package: org.apache.drill.exec.manual
 */
public class TestManualLPGroupByUser {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestManualLPGroupByUser.class);

  // @Test
  public void testManualStaticLogicalPlan() throws Exception {
    DrillConfig c = DrillConfig.create();
    UUID uuid = UUID.randomUUID();
    UserBitShared.QueryId id = UserBitShared.QueryId.newBuilder().setPart1(uuid.getMostSignificantBits())
      .setPart2(uuid.getLeastSignificantBits()).build();
    DrillbitContext dbc;
    QueryContext qc;
    String projectId = "ddt";
    String date = "20121201";
    String event = "visit.*";
    ManualStaticLPBuilder.Grouping g = ManualStaticLPBuilder.Grouping.buildUserGroup("language");
    LogicalPlan logicalPlan = ManualStaticLPBuilder.buildStaticLogicalPlanManually(projectId, event, date, null, g);
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         Drillbit bit = new Drillbit(c, serviceSet);
         DrillClient client = new DrillClient(c, serviceSet.getCoordinator());) {
      bit.run();
      client.connect();
      dbc = bit.getContext();
      qc = new QueryContext(id, dbc);

      BasicOptimizer bo = new BasicOptimizer(c, qc);
      PhysicalPlan physicalPlan = bo.optimize(new BasicOptimizer.BasicOptimizationContext(), logicalPlan);
      System.out.println(physicalPlan.unparse(c.getMapper().writer()));
      System.out.println("---------------------------------");

//      RecordBatchLoader batchLoader = new RecordBatchLoader(bit.getContext().getAllocator());
//      List<QueryResultBatch> results = client
//        .runQuery(UserProtos.QueryType.PHYSICAL, physicalPlan.unparse(c.getMapper().writer()));
//      int recordCount = 0;
//      for (QueryResultBatch batch : results) {
//        if (batch.hasData()) {
//          continue;
//        }
//
//        boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
//        boolean firstColumn = true;
//
//        // print headers.
//        if (schemaChanged) {
//          System.out.println("\n\n========NEW SCHEMA=========\n\n");
//          for (IntObjectCursor<ValueVector<?>> v : batchLoader) {
//            if (firstColumn) {
//              firstColumn = false;
//            } else {
//              System.out.print("\t");
//            }
//            System.out.print(v.value.getField().getName());
//            System.out.print("[");
//            System.out.print(v.value.getField().getType().getMinorType());
//            System.out.print("]");
//          }
//          System.out.println();
//        }
//
//        for (int i = 0; i < batchLoader.getRecordCount(); i++) {
//          boolean first = true;
//          recordCount++;
//          for (IntObjectCursor<ValueVector<?>> v : batchLoader) {
//            if (first) {
//              first = false;
//            } else {
//              System.out.print("\t");
//            }
//            System.out.print(v.value.getObject(i));
//          }
//          if (!first) {
//            System.out.println();
//          }
//        }
//      }
    }
  }
}
