package org.apache.drill.exec.opt;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_B_DATE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_EVENT;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_E_DATE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROPERTY;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROPERTY_VALUE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.LogicalPlan;

import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Segment;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;

import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.PhysicalOperator;

import org.apache.drill.exec.physical.config.MockScanPOP;
import org.apache.drill.exec.physical.config.Screen;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.drill.exec.physical.config.Group;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.HbaseUserScanPOP;
import org.apache.drill.exec.physical.config.PhysicalCollapsingAggregate;
import org.apache.drill.exec.physical.config.PhysicalJoin;
import org.apache.drill.exec.physical.config.Screen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Created with IntelliJ IDEA. User: jaltekruse Date: 6/11/13 Time: 5:32 PM To change this template use File | Settings
 * | File Templates.
 */
public class BasicOptimizer extends Optimizer {

  private DrillConfig config;
  private QueryContext context;

  public BasicOptimizer(DrillConfig config, QueryContext context) {
    this.config = config;
    this.context = context;
  }

  @Override
  public void init(DrillConfig config) {

  }

  @Override
  public PhysicalPlan optimize(OptimizationContext context, LogicalPlan plan) {
    Object obj = new Object();
    Collection<SinkOperator> roots = plan.getGraph().getRoots();
    List<PhysicalOperator> physOps = new ArrayList<PhysicalOperator>(roots.size());
    LogicalConverter converter = new LogicalConverter();
    for (SinkOperator op : roots) {
      try {
        PhysicalOperator pop = op.accept(converter, obj);
        physOps.add(pop);
      } catch (OptimizerException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } catch (Throwable throwable) {
        throwable.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

    PlanProperties props = new PlanProperties();
    props.type = PlanProperties.PlanType.APACHE_DRILL_PHYSICAL;
    props.version = plan.getProperties().version;
    props.generator = plan.getProperties().generator;
    return new PhysicalPlan(props, physOps);
  }

  @Override
  public void close() {

  }

  public static class BasicOptimizationContext implements OptimizationContext {

    @Override
    public int getPriority() {
      return 1;
    }
  }

  private class LogicalConverter extends AbstractLogicalVisitor<PhysicalOperator, Object, OptimizerException> {

    @Override
    public PhysicalOperator visitScan(Scan scan, Object obj) throws OptimizerException {
      String storageEngine = scan.getStorageEngine();
      if (!SE_HBASE.equals(storageEngine)) {
        throw new OptimizerException("Unsupported storage engine - " + storageEngine);
      }
      JSONOptions selection = scan.getSelection();
      JsonNode root = selection.getRoot();
      String table;
      table = root.get(SELECTION_KEY_WORD_TABLE).textValue();

      if (table.contains("deu")) {
        String realBeginDate = root.get(SELECTION_KEY_WORD_B_DATE).textValue();
        String realEndDate = root.get(SELECTION_KEY_WORD_E_DATE).textValue();
        String event = root.get(SELECTION_KEY_WORD_EVENT).textValue();
        return new HbaseScanPOP(
          Arrays.asList(new HbaseScanPOP.HbaseScanEntry(table, realBeginDate, realEndDate, event)));
      } else {
        HbaseUserScanPOP.HbaseUserScanEntry userScanEntry;
        String prop = root.get(SELECTION_KEY_WORD_PROPERTY).textValue();
        String propValue = null;
        if (root.has(SELECTION_KEY_WORD_PROPERTY_VALUE)) {
          propValue = root.get(SELECTION_KEY_WORD_PROPERTY_VALUE).textValue();
        }
        if (StringUtils.isBlank(propValue)) {
          userScanEntry = new HbaseUserScanPOP.HbaseUserScanEntry(table, prop, null);
        } else {
          userScanEntry = new HbaseUserScanPOP.HbaseUserScanEntry(table, prop, propValue);
        }
        return new HbaseUserScanPOP(Arrays.asList(userScanEntry));
      }
    }

    @Override
    public Screen visitStore(Store store, Object obj) throws OptimizerException {
      if (!store.iterator().hasNext()) {
        throw new OptimizerException("Store node in logical plan does not have a child.");
      }
      LogicalOperator next = store.iterator().next();
      return new Screen(next.accept(this, obj), context.getCurrentEndpoint());
    }

    @Override
    public PhysicalOperator visitProject(Project project, Object obj) throws OptimizerException {
      return project.getInput().accept(this, obj);
    }

    @Override
    public PhysicalOperator visitCollapsingAggregate(CollapsingAggregate collapsingAggregate, Object value) throws
      OptimizerException {
      LogicalOperator next = collapsingAggregate.iterator().next();
      FieldReference target = collapsingAggregate.getTarget();
      FieldReference within = collapsingAggregate.getWithin();
      FieldReference[] carryovers = collapsingAggregate.getCarryovers();
      NamedExpression[] aggregations = collapsingAggregate.getAggregations();
      PhysicalCollapsingAggregate pca = new PhysicalCollapsingAggregate(next.accept(this, value), within, target,
                                                                        carryovers, aggregations);
      return pca;
    }

    @Override
    public PhysicalOperator visitFilter(Filter filter, Object value) throws OptimizerException {
      LogicalOperator lo = filter.iterator().next();
      LogicalExpression le = filter.getExpr();
      org.apache.drill.exec.physical.config.Filter f = new org.apache.drill.exec.physical.config.Filter(
        lo.accept(this, value), le, 0.5f);
      return f;
    }


    @Override
    public PhysicalOperator visitJoin(Join join, Object value) throws OptimizerException {
      LogicalOperator leftLO = join.getLeft();
      LogicalOperator rightLO = join.getRight();
      JoinCondition singleJoinCondition = join.getConditions()[0];
      PhysicalOperator leftPOP = leftLO.accept(this, value);
      PhysicalOperator rightPOP = rightLO.accept(this, value);

      PhysicalJoin joinPOP = new PhysicalJoin(leftPOP, rightPOP, singleJoinCondition);
      return joinPOP;
    }

    @Override
    public PhysicalOperator visitSegment(Segment segment, Object value) throws OptimizerException {
      LogicalOperator next = segment.iterator().next();
      Group segmentPOP = new Group(next.accept(this, value), segment.getExprs(), segment.getName());
      return segmentPOP;
    }

  }
}
