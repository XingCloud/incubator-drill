package org.apache.drill.outer.manual.logical;

import static org.apache.drill.outer.manual.ManualStaticLPBuilder.buildStaticLogicalPlanManually;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu
 * Date: 13-8-2
 * Time: 上午11:04
 * Package: org.apache.drill.outer.manual.logical
 */
public class TestBuildLogicalPlanCommonWithSegment {

  @Test
  public void buildLogical1() throws IOException, TException {
    DrillConfig c = DrillConfig.create();
    Map<String, Object> segmentMap = new HashMap<>(1);
    segmentMap.put("register_time", "2013-07-12");
    segmentMap.put("language", "zh_cn");
    segmentMap.put("ref", "google");
    LogicalPlan logicalPlan = buildStaticLogicalPlanManually("age", "visit.*", "20130701", segmentMap, null);
    System.out.println(logicalPlan.toJsonString(c));
  }

}
