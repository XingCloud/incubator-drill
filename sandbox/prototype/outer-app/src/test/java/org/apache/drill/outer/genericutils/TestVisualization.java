package org.apache.drill.outer.genericutils;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.outer.manual.PlanMerge;
import org.apache.drill.outer.utils.GraphVisualize;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class TestVisualization {
  DrillConfig c = DrillConfig.create();

  
  @Test
  public void testVisualization()throws Exception{
    LogicalPlan plan2 = readPlan("/common.segm.json");
    LogicalPlan plan = readPlan("/common.nosegm.json");
    List<LogicalPlan> merged = PlanMerge.sortAndMerge(Arrays.asList(plan, plan2));
    GraphVisualize.visualize(merged.get(0));
  }

  private LogicalPlan readPlan(String path) throws IOException {
    return LogicalPlan.parse(c, FileUtils.getResourceAsString(path));
  }

  
}
