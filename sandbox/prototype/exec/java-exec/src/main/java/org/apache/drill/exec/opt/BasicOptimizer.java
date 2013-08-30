package org.apache.drill.exec.opt;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.common.logical.data.UnionedScan;
import org.apache.drill.common.logical.data.UnionedScanSplit;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.store.HBaseStorageEngine;
import org.apache.drill.exec.store.StorageEngine;
import org.apache.drill.exec.physical.config.CollapsingAggregatePOP;
import org.apache.drill.exec.physical.config.JoinPOP;
import org.apache.drill.exec.physical.config.MysqlScanPOP;
import org.apache.drill.exec.physical.config.SegmentPOP;
import org.apache.drill.exec.physical.config.UnionedScanSplitPOP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class BasicOptimizer extends Optimizer{

  private DrillConfig config;
  private QueryContext context;

  public BasicOptimizer(DrillConfig config, QueryContext context){
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
    LogicalConverter converter = new LogicalConverter(plan);
    for ( SinkOperator op : roots){
      try {
        PhysicalOperator pop  = op.accept(converter, obj);
        System.out.println(pop);

        physOps.add(pop);
      } catch (OptimizerException e) {
        e.printStackTrace();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
    }

    PlanProperties props = new PlanProperties();
    props.type = PlanProperties.PlanType.APACHE_DRILL_PHYSICAL;
    props.version = plan.getProperties().version;
    props.generator = plan.getProperties().generator;
    PhysicalPlan p = new PhysicalPlan(props, physOps);
    return p;
    //return new PhysicalPlan(props, physOps);
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

    // storing a reference to the plan for access to other elements outside of the query graph
    // such as the storage engine configs
    LogicalPlan logicalPlan;
    Map<LogicalOperator, PhysicalOperator> operatorMap = Maps.newHashMap();
    
    
    public LogicalConverter(LogicalPlan logicalPlan){
      this.logicalPlan = logicalPlan;
    }


    @Override
    public PhysicalOperator visitScan(Scan scan, Object obj) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(scan);
      if (pop == null) {
        //immars todo Storage Engine Implementation
        StorageEngineConfig config = logicalPlan.getStorageEngineConfig(scan.getStorageEngine());
        if (config == null)
          throw new OptimizerException(String.format("Logical plan referenced the storage engine config %s but the logical plan didn't have that available as a config.", scan.getStorageEngine()));
        StorageEngine engine;
        try {
          engine = context.getStorageEngine(config);
          pop = engine.getPhysicalScan(scan, BasicOptimizer.this.context);
          operatorMap.put(scan, pop);
        } catch (SetupException | IOException e) {
          throw new OptimizerException("Failure while attempting to retrieve storage engine.", e);
        }

      }
      return pop;
    }

    @Override
    public Screen visitStore(Store store, Object obj) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(store);
      if(pop == null){
      if (!store.iterator().hasNext()) {
        throw new OptimizerException("Store node in logical plan does not have a child.");
      }
      LogicalOperator next = store.iterator().next();
      pop = new Screen(next.accept(this, obj), context.getCurrentEndpoint());
      }
      return (Screen) pop;
    }

    @Override
     public PhysicalOperator visitProject(Project project, Object obj) throws OptimizerException {
       PhysicalOperator pop = operatorMap.get(project);
       if (pop == null) {
         pop = new org.apache.drill.exec.physical.config.Project(Arrays.asList(project.getSelections()),
                                                                 project.getInput().accept(this, obj));
         operatorMap.put(project, pop);
       }
       return pop;
     }

    @Override
    public PhysicalOperator visitFilter(Filter filter, Object value) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(filter);
      if (pop == null) {
        LogicalOperator lo = filter.iterator().next();
        LogicalExpression le = filter.getExpr();
        pop = new org.apache.drill.exec.physical.config.Filter(lo.accept(this, value), le, 0.5f);
        operatorMap.put(filter, pop);
      }
      return pop;
    }

    public PhysicalOperator visitUnionedScan(UnionedScan scan, Object value) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(scan);
      ObjectMapper mapper = context.getConfig().getMapper();
      if (pop == null) {
        try {
          pop = HBaseStorageEngine.getInstance().getPhysicalScan(scan, BasicOptimizer.this.context);
          operatorMap.put(scan, pop);
        } catch (IOException e) {
          throw new OptimizerException("create pop for UnionedScan failed!", e);
        }
        operatorMap.put(scan, pop);
      }
      return pop;
    }

    

    @Override
    public PhysicalOperator visitUnionedScanSplit(UnionedScanSplit scanSplit, Object value) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(scanSplit);
      if (pop == null) {
        pop = new UnionedScanSplitPOP(scanSplit.getInput().accept(this, value), scanSplit.getEntries());
        operatorMap.put(scanSplit, pop);
      }
      return pop;
    }

    @Override
    public PhysicalOperator visitCollapsingAggregate(CollapsingAggregate collapsingAggregate, Object value) throws
      OptimizerException {
      PhysicalOperator pop = operatorMap.get(collapsingAggregate);
      if (pop == null) {
        LogicalOperator next = collapsingAggregate.iterator().next();
        FieldReference target = collapsingAggregate.getTarget();
        FieldReference within = collapsingAggregate.getWithin();
        FieldReference[] carryovers = collapsingAggregate.getCarryovers();
        NamedExpression[] aggregations = collapsingAggregate.getAggregations();
        System.out.println(next);
        pop = new CollapsingAggregatePOP(next.accept(this, value), within, target, carryovers, aggregations);
        operatorMap.put(collapsingAggregate, pop);
      }
      return pop;
    }



    @Override
    public PhysicalOperator visitJoin(org.apache.drill.common.logical.data.Join join, Object value) throws
      OptimizerException {
      PhysicalOperator pop = operatorMap.get(join);
      if (pop == null) {
        LogicalOperator leftLO = join.getLeft();
        LogicalOperator rightLO = join.getRight();
        JoinCondition singleJoinCondition = join.getConditions()[0];
        PhysicalOperator leftPOP = leftLO.accept(this, value);
        PhysicalOperator rightPOP = rightLO.accept(this, value);
        Join.JoinType joinType = join.getJointType();
        pop = new JoinPOP(leftPOP, rightPOP, singleJoinCondition, joinType.name());
        operatorMap.put(join, pop);
      }
      return pop;
    }

    @Override
    public PhysicalOperator visitSegment(org.apache.drill.common.logical.data.Segment segment, Object value) throws
      OptimizerException {
      PhysicalOperator pop = operatorMap.get(segment);
      if (pop == null) {
        LogicalOperator next = segment.iterator().next();
        pop = new SegmentPOP(next.accept(this, value), segment.getExprs(), segment.getName());
        operatorMap.put(segment, pop);
      }
      return pop;
    }

    @Override
    public PhysicalOperator visitUnion(Union union, Object value) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(union);
      if (pop == null) {

        LogicalOperator logicalOperators[] = union.getInputs();
        PhysicalOperator inputs[] = new PhysicalOperator[logicalOperators.length];
        for (int i = 0; i < logicalOperators.length; i++) {
          PhysicalOperator input = operatorMap.get(logicalOperators[i]);
          if (input == null) {
            input = logicalOperators[i].accept(this, value);
            operatorMap.put(logicalOperators[i], input);
          }
          inputs[i] = input;
        }
        pop = new org.apache.drill.exec.physical.config.Union(inputs);
        operatorMap.put(union, pop);
      }
      return pop;
    }
  }
}
