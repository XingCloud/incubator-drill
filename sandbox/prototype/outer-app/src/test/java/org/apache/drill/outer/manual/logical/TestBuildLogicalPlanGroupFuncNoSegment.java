package org.apache.drill.outer.manual.logical;

import static org.apache.drill.outer.manual.ManualStaticLPBuilder.buildStaticLogicalPlanManually;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.outer.manual.ManualStaticLPBuilder;
import org.junit.Test;

/**
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 上午11:04
 * Package: org.apache.drill.outer.manual.logical
 */
public class TestBuildLogicalPlanGroupFuncNoSegment extends LogicalTestBase {

  @Test
  public void buildLogical1() throws Exception {
    DrillConfig c = DrillConfig.create();
    LogicalPlan logicalPlan = buildStaticLogicalPlanManually("testtable_100w", "visit.*", "20130701", null,
      ManualStaticLPBuilder.Grouping.buildFuncGroup("hour", "timestamp"));
    System.out.println(logicalPlan.toJsonString(c));

    PhysicalPlan physicalPlan = convert2Physical(c, logicalPlan);
    System.out.println(physicalPlan.unparse(c.getMapper().writer()));
  }

}
