package org.apache.drill.exec.opt;

import static org.apache.drill.common.util.DrillConstants.SE_HBASE;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.MockScanPOP;
import org.apache.drill.exec.physical.config.MockScanPOP.MockColumn;
import org.apache.drill.exec.physical.config.PhysicalCollapsingAggregate;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.proto.SchemaDefProtos;

import java.util.ArrayList;
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
    System.out.println("----------------" + roots);
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
    public MockScanPOP visitScan(Scan scan, Object obj) throws OptimizerException {
      List<MockScanPOP.MockScanEntry> modkObjects = null;

      String storageEngine = scan.getStorageEngine();
      JSONOptions selection = scan.getSelection();

      if (SE_HBASE.equals(storageEngine)) {
        modkObjects = new ArrayList<>();
        MockColumn[] cols = {
          new MockColumn("uid", SchemaDefProtos.MinorType.VARCHAR1, SchemaDefProtos.DataMode.REQUIRED, 4, 4, 4),
          new MockColumn("date", SchemaDefProtos.MinorType.DATE, SchemaDefProtos.DataMode.REQUIRED, 4, 4, 4),
          new MockColumn("l0", SchemaDefProtos.MinorType.VARCHAR1, SchemaDefProtos.DataMode.REQUIRED, 4, 4, 4),
        };
        modkObjects.add(new MockScanPOP.MockScanEntry(100, cols));
      }
      return new MockScanPOP("http://a.xingcloud.com", modkObjects);
    }

    @Override
    public Screen visitStore(Store store, Object obj) throws OptimizerException {
      if (!store.iterator().hasNext()) {
        throw new OptimizerException("Store node in logical plan does not have a child.");
      }
      return new Screen(store.iterator().next().accept(this, obj), context.getCurrentEndpoint());
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
  }
}
