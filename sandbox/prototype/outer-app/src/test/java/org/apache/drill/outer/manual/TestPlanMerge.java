package org.apache.drill.outer.manual;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.outer.manual.ManualStaticLPBuilder.buildStaticLogicalPlanManually;

public class TestPlanMerge {
  
  @Test
  public void testMerge1() throws Exception{
    DrillConfig c = DrillConfig.create();
        Map<String, Object> segmentMap = new HashMap<>(1);
        segmentMap.put("register_time", "2013-07-12");
    List<LogicalPlan> plans = new ArrayList<>();
    for (int i = 1; i < 10; i++) {
      LogicalPlan logicalPlan = buildStaticLogicalPlanManually(c, "age", "visit.*", "2013070"+i, segmentMap,
        ManualStaticLPBuilder.Grouping.buildFuncGroup("hour", "timestamp")); 
      plans.add(logicalPlan);
    }
    plans = PlanMerge.sortAndMerge(plans);
  }

  
}
