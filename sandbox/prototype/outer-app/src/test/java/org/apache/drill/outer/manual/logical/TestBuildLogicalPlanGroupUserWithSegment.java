package org.apache.drill.outer.manual.logical;

import static org.apache.drill.outer.manual.ManualStaticLPBuilder.buildStaticLogicalPlanManually;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.outer.manual.ManualStaticLPBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 上午11:04
 * Package: org.apache.drill.outer.manual.logical
 */
public class TestBuildLogicalPlanGroupUserWithSegment extends LogicalTestBase {

  @Test
  public void buildLogical1() throws Exception {
    DrillConfig c = DrillConfig.create();
    Map<String, Object> segmentMap = new HashMap<>(1);
    segmentMap.put("register_time", "2013-07-12");
    LogicalPlan logicalPlan = buildStaticLogicalPlanManually("age", "visit.*", "20130701", segmentMap,
      ManualStaticLPBuilder.Grouping.buildUserGroup("nation"));
    System.out.println(logicalPlan.toJsonString(c));

    PhysicalPlan physicalPlan = convert2Physical(c, logicalPlan);
    System.out.println(physicalPlan.unparse(c.getMapper().writer()));
  }

}
