package org.apache.drill.outer.manual.logical;

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
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 下午6:27
 * Package: org.apache.drill.outer.manual.logical
 */
public class LogicalTestBase {

  protected PhysicalPlan convert2Physical(DrillConfig c, LogicalPlan logicalPlan) throws Exception {
    UUID uuid = UUID.randomUUID();
    UserBitShared.QueryId id = UserBitShared.QueryId.newBuilder().setPart1(uuid.getMostSignificantBits())
      .setPart2(uuid.getLeastSignificantBits()).build();
    DrillbitContext dbc;
    QueryContext qc;
    try (RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
         Drillbit bit = new Drillbit(c, serviceSet);
         DrillClient client = new DrillClient(c, serviceSet.getCoordinator());) {
      bit.run();
      client.connect();
      dbc = bit.getContext();
      qc = new QueryContext(id, dbc);

      BasicOptimizer bo = new BasicOptimizer(c, qc);
      return bo.optimize(new BasicOptimizer.BasicOptimizationContext(), logicalPlan);
    }
  }
}
