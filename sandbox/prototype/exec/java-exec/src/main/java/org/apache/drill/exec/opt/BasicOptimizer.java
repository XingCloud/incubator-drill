package org.apache.drill.exec.opt;

import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.DrillConstants.SE_MYSQL;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTER;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTERS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTER_TYPE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROJECTIONS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_END;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_START;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;
import static org.apache.drill.exec.physical.config.HbaseScanPOP.HbaseScanEntry;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.xingcloud.hbase.util.Constants;
import com.xingcloud.hbase.util.RowKeyUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.CollapsingAggregate;
import org.apache.drill.common.logical.data.Filter;
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
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.CollapsingAggregatePOP;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.JoinPOP;
import org.apache.drill.exec.physical.config.MysqlScanPOP;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.physical.config.SegmentPOP;
import org.apache.drill.exec.physical.config.UnionedScanPOP;
import org.apache.drill.exec.physical.config.UnionedScanSplitPOP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
        e.printStackTrace();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
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

    Map<LogicalOperator, PhysicalOperator> operatorMap = Maps.newHashMap();

    @Override
    public PhysicalOperator visitUnionedScan(UnionedScan scan, Object value) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(scan);
      ObjectMapper mapper = context.getConfig().getMapper();
      if (pop == null) {
        JSONOptions selection = scan.getSelection();
        FieldReference ref = scan.getOutputReference();
        if (selection == null) {
          throw new OptimizerException("Selection is null");
        }
        List<HbaseScanEntry> entries = new ArrayList<>();
        createHbaseScanEntry(selection, ref, entries);
        pop = new UnionedScanPOP(entries);
        operatorMap.put(scan, pop);
      }
      return pop;
    }

    private void createHbaseScanEntry(JSONOptions selections, FieldReference ref, List<HbaseScanEntry> entries) throws
      OptimizerException {
      //TODO where is ref?
      ObjectMapper mapper = BasicOptimizer.this.config.getMapper();
      JsonNode root = selections.getRoot(), filters, projections, rowkey;
      String table, rowkeyStart, rowkeyEnd, projectionString, filterString, filterType;
      int selectionSize = root.size();
      HbaseScanPOP.HbaseScanEntry entry;
      List<LogicalExpression> filterList;
      List<NamedExpression> projectionList;
      NamedExpression ne;
      List<HbaseScanPOP.RowkeyFilterEntry> filterEntries;
      HbaseScanPOP.RowkeyFilterEntry rowkeyFilterEntry;
      Constants.FilterType filterTypeFR;
      for (JsonNode selection : root) {
        // Table name
        table = selection.get(SELECTION_KEY_WORD_TABLE).textValue();

        // Rowkey range
        rowkey = selection.get(SELECTION_KEY_WORD_ROWKEY);
        rowkeyStart = rowkey.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
        //rowkeyStart = RowKeyUtils.processRkBound(rowkeyStart, true);
        //rowkeyStart = Bytes.toStringBinary(RowKeyUtils.appendBytes(Bytes.toBytesBinary(rowkeyStart),RowKeyUtils.produceTail(true)));
        rowkeyEnd = rowkey.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();
        //rowkeyEnd = RowKeyUtils.processRkBound(rowkeyEnd, false);
        //rowkeyEnd = Bytes.toStringBinary(RowKeyUtils.appendBytes(Bytes.toBytesBinary(rowkeyEnd),RowKeyUtils.produceTail(false)));

        // Filters
        filters = selection.get(SELECTION_KEY_WORD_FILTERS);
        if (filters != null) {
          filterEntries = new ArrayList<>(filters.size());
          for (JsonNode filterNode : filters) {
            filterType = filterNode.get(SELECTION_KEY_WORD_FILTER_TYPE).textValue();
            if ("ROWKEY".equals(filterType)) {
              JsonNode includes = filterNode.get(SELECTION_KEY_WORD_ROWKEY_INCLUDES);
              if (includes == null || includes.size() == 0) {
                throw new OptimizerException("Rowkey filter must have at least one include.");
              }
              filterList = new ArrayList<>(includes.size());
              for (JsonNode include : includes) {
                filterString = include.textValue();
                filterString = filterString.substring(1, filterString.length() - 1);
                filterList.add(new ValueExpressions.QuotedString(filterString, ExpressionPosition.UNKNOWN));
              }
              filterTypeFR = Constants.FilterType.XaRowKeyPattern;
              rowkeyFilterEntry = new HbaseScanPOP.RowkeyFilterEntry(filterTypeFR, filterList);
              filterEntries.add(rowkeyFilterEntry);
            }
          }
        } else {
          filterEntries = null;
        }

        // Projections
        projections = selection.get(SELECTION_KEY_WORD_PROJECTIONS);
        projectionList = new ArrayList<>(projections.size());
        for (JsonNode projectionNode : projections) {
          projectionString = projectionNode.toString();
          try {
            ne = mapper.readValue(projectionString, NamedExpression.class);
          } catch (IOException e) {
            throw new OptimizerException("Cannot parse projection - " + projectionString);
          }
          projectionList.add(ne);
        }
        entry = new HbaseScanEntry(table, rowkeyStart, rowkeyEnd, filterEntries, projectionList);
        entries.add(entry);
      }
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
    public PhysicalOperator visitScan(Scan scan, Object obj) throws OptimizerException {
      PhysicalOperator pop = operatorMap.get(scan);
      if (pop == null) {

        String storageEngine = scan.getStorageEngine();
        if (SE_HBASE.equals(storageEngine)) {
          JSONOptions selections = scan.getSelection();
          if (selections == null) {
            throw new OptimizerException("Selection is null");
          }
          List<HbaseScanEntry> entries = new ArrayList<>(selections.getRoot().size());
          createHbaseScanEntry(selections, scan.getOutputReference(), entries);
          pop = new HbaseScanPOP(entries);
        } else if (SE_MYSQL.equals(storageEngine)) {
          JSONOptions root = scan.getSelection();
          if (root == null) {
            throw new OptimizerException("Selection is null");
          }
          JsonNode selection = root.getRoot(), projections;
          String tableName, filter = null;
          List<MysqlScanPOP.MysqlReadEntry> readEntries = Lists.newArrayList();
          List<NamedExpression> projectionList = Lists.newArrayList();
          tableName = selection.get(SELECTION_KEY_WORD_TABLE).textValue();
          if (selection.get(SELECTION_KEY_WORD_FILTER) != null)
            filter = selection.get(SELECTION_KEY_WORD_FILTER).textValue();
          projections = selection.get(SELECTION_KEY_WORD_PROJECTIONS);
          ObjectMapper mapper = BasicOptimizer.this.config.getMapper();
          for (JsonNode projection : projections) {
            try {
              projectionList.add(mapper.readValue(projection.toString(), NamedExpression.class));
            } catch (IOException e) {
              throw new OptimizerException("Cannot parse projection : " + projection.toString());
            }
          }
          readEntries.add(new MysqlScanPOP.MysqlReadEntry(tableName, filter, projectionList));
          pop = new MysqlScanPOP(readEntries);

        } else {
          throw new OptimizerException("Unsupported storage engine - " + storageEngine);
        }
        operatorMap.put(scan, pop);
      }
      return pop;
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
      PhysicalOperator pop = operatorMap.get(project);
      if (pop == null) {
        pop = new org.apache.drill.exec.physical.config.Project(Arrays.asList(project.getSelections()),
                                                                project.getInput().accept(this, obj));
        operatorMap.put(project, pop);
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
