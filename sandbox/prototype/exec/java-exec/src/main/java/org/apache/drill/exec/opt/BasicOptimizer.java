package org.apache.drill.exec.opt;

import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTERS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROJECTIONS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_END;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_START;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;
import static org.apache.drill.exec.physical.config.HbaseScanPOP.HbaseScanEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.runtime.RecognitionException;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.LogicalExpressionParser;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.*;
import org.apache.drill.exec.physical.config.HbaseScanPOP;

import java.io.IOException;
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
      JSONOptions selections = scan.getSelection();
      if (selections == null) {
        throw new OptimizerException("Selection is null");
      }

      ObjectMapper mapper = DrillConfig.create().getMapper();
      JsonNode root = selections.getRoot(), filters, projections, rowkey;
      String table, rowkeyStart, rowkeyEnd, projectionString, filterString;
      int selectionSize = root.size();

      HbaseScanPOP.HbaseScanEntry entry;
      List<HbaseScanEntry> entries = new ArrayList<>(selectionSize);
      List<LogicalExpression> filterList;
      LogicalExpression le;
      List<NamedExpression> projectionList;
      NamedExpression ne;

      for (JsonNode selection : root) {
        table = selection.get(SELECTION_KEY_WORD_TABLE).textValue();
        rowkey = selection.get(SELECTION_KEY_WORD_ROWKEY);
        rowkeyStart = rowkey.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
        rowkeyEnd = rowkey.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();

        filters = selection.get(SELECTION_KEY_WORD_FILTERS);

        filterList = new ArrayList<>(filters.size());
        for (JsonNode filterNode : filters) {
          filterString = filterNode.textValue();
          try {
            le = LogicalExpressionParser.parse(filterString);
          } catch (RecognitionException e) {
            throw new OptimizerException("Cannot parse filter - " + filterString);
          }
          filterList.add(le);
        }
        projections = root.get(SELECTION_KEY_WORD_PROJECTIONS);
        projectionList = new ArrayList<>(projections.size());

        for (JsonNode projectionNode : projections) {
          projectionString = projectionNode.textValue();
          try {
            ne = mapper.readValue(projectionString, NamedExpression.class);
          } catch (IOException e) {
            throw new OptimizerException("Cannot parse projection - " + projectionString);
          }
          projectionList.add(ne);
        }
        entry = new HbaseScanEntry(table, rowkeyStart, rowkeyEnd, filterList, projectionList);
        entries.add(entry);
      }
      return new HbaseScanPOP(entries);
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
    public PhysicalOperator visitJoin(org.apache.drill.common.logical.data.Join join, Object value) throws OptimizerException {
      LogicalOperator leftLO = join.getLeft();
      LogicalOperator rightLO = join.getRight();
      JoinCondition singleJoinCondition = join.getConditions()[0];
      PhysicalOperator leftPOP = leftLO.accept(this, value);
      PhysicalOperator rightPOP = rightLO.accept(this, value);

      // TODO join type
      JoinPOP joinPOP = new JoinPOP(leftPOP, rightPOP, singleJoinCondition,null);
      return joinPOP;
    }

    @Override
    public PhysicalOperator visitSegment(org.apache.drill.common.logical.data.Segment segment, Object value) throws OptimizerException {
      LogicalOperator next = segment.iterator().next();
      SegmentPOP segmentPOP = new SegmentPOP(next.accept(this, value), segment.getExprs(), segment.getName());
      return segmentPOP;
    }

  }
}
