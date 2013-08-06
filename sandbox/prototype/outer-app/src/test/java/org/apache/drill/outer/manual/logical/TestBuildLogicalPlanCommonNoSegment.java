package org.apache.drill.outer.manual.logical;

import static org.apache.drill.outer.manual.ManualStaticLPBuilder.buildStaticLogicalPlanManually;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.physical.PhysicalPlan;
import static org.apache.drill.outer.physical.TestPhysicalPlan.runNoExchangeFragment;
import org.junit.Test;

/**
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 上午11:04
 * Package: org.apache.drill.outer.manual.logical
 */
public class TestBuildLogicalPlanCommonNoSegment extends LogicalTestBase {

  @Test
  public void buildLogical1() throws Exception {
    DrillConfig c = DrillConfig.create();
    LogicalPlan logicalPlan = buildStaticLogicalPlanManually(c, "age", "visit.*", "20130701", null, null);
    System.out.println(logicalPlan.toJsonString(c));

    PhysicalPlan physicalPlan = convert2Physical(c, logicalPlan);
    System.out.println(physicalPlan.unparse(c.getMapper().writer()));
    runNoExchangeFragment(physicalPlan.unparse(c.getMapper().writer()));

  }

}
