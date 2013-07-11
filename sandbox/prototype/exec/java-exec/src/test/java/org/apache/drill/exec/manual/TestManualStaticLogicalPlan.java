package org.apache.drill.exec.manual;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.manual.ManualStaticLPBuilder;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import java.util.UUID;

/**
 * User: Z J Wu Date: 13-7-9 Time: 上午10:24 Package: org.apache.drill.exec.manual
 */
public class TestManualStaticLogicalPlan extends PopUnitTestBase {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestManualStaticLogicalPlan.class);

  @Test
  public void testManualStaticLogicalPlan() throws Exception {
    DrillConfig c = DrillConfig.create();
    UUID uuid = UUID.randomUUID();
    UserBitShared.QueryId id = UserBitShared.QueryId.newBuilder().setPart1(uuid.getMostSignificantBits())
                                            .setPart2(uuid.getLeastSignificantBits()).build();
    DrillbitContext dbc;
    QueryContext qc;
    String projectId = "ddt";
    String date = "20130709";
    String event = "a.b.c.*";
    LogicalPlan logicalPlan = ManualStaticLPBuilder.buildStaticLogicalPlanManually(projectId, event, date);
    System.out.println(logicalPlan.toJsonString(c));
    System.out.println("---------------------------------");
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         Drillbit bit = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {
      bit.run();
      client.connect();
      dbc = bit.getContext();
      qc = new QueryContext(id, dbc);

      BasicOptimizer bo = new BasicOptimizer(c, qc);
      PhysicalPlan physicalPlan = bo.optimize(new BasicOptimizer.BasicOptimizationContext(), logicalPlan);
      System.out.println(physicalPlan.unparse(c.getMapper().writer()));
    }
  }
}
