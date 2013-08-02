package org.apache.drill.outer.manual.logical;

import static org.apache.drill.outer.manual.ManualStaticLPBuilder.buildStaticLogicalPlanManually;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.outer.manual.ManualStaticLPBuilder;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;

/**
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 上午11:04
 * Package: org.apache.drill.outer.manual.logical
 */
public class TestBuildLogicalPlanGroupUserNoSegment {

  @Test
  public void buildLogical1() throws IOException, TException {
    DrillConfig c = DrillConfig.create();
    LogicalPlan logicalPlan = buildStaticLogicalPlanManually("age", "visit.*", "20130701", null,
      ManualStaticLPBuilder.Grouping.buildUserGroup("nation"));
    System.out.println(logicalPlan.toJsonString(c));
  }

}
