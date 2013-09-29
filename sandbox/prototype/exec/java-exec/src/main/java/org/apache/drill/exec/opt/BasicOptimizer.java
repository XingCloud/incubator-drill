package org.apache.drill.exec.opt;

import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.DrillConstants.SE_MYSQL;
import static org.apache.drill.common.util.Selections.*;
import static org.apache.drill.exec.physical.config.HbaseScanPOP.HbaseScanEntry;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.Maps;
import com.xingcloud.events.XEvent;
import com.xingcloud.events.XEventException;
import com.xingcloud.events.XEventOperation;
import com.xingcloud.hbase.util.Constants;
import com.xingcloud.hbase.util.RowKeyUtils;
import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.meta.HBaseFieldInfo;

import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
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
import org.apache.drill.exec.util.logicalplan.LogicalPlanUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

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
      JsonNode root = selections.getRoot(), filter, rowkey, projections, filters;
      String table, rowkeyStart, rowkeyEnd, tailSrt, tailEnd, projectionString, filterString, filterType;
      int selectionSize = root.size();
      HbaseScanPOP.HbaseScanEntry entry;
      List<LogicalExpression> filterList;
      List<NamedExpression> projectionList;
      NamedExpression ne;

      HbaseScanPOP.RowkeyFilterEntry rowkeyFilterEntry;
      Constants.FilterType filterTypeFR;
      for (JsonNode selection : root) {
        // Table name
        table = selection.get(SELECTION_KEY_WORD_TABLE).textValue();
        // Rowkey range
        rowkey = selection.get(SELECTION_KEY_WORD_ROWKEY);
        String filterExpr = selection.get(SELECTION_KEY_WORD_FILTER).textValue();
        rowkeyStart = rowkey.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
        rowkeyEnd = rowkey.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();
        JsonNode tail = selection.get(SELECTION_KEY_ROWKEY_TAIL_RANGE);
        if (tail != null && !(tail instanceof NullNode)) {
          tailSrt = tail.get(SELECTION_KEY_ROWKEY_TAIL_START).textValue();
          tailEnd = tail.get(SELECTION_KEY_ROWKEY_TAIL_END).textValue();
        } else {
          tailSrt = "\\x00\\x00\\x00\\x00\\x00";
          tailEnd = "\\xFF\\xFF\\xFF\\xFF\\xFF";
        }
        //rowkeyStart+=tailSrt;
        //rowkeyEnd+=tailEnd;
        // Filters
        List<HbaseScanPOP.RowkeyFilterEntry> filterEntries = new ArrayList<>();
        filter = selection.get(SELECTION_KEY_WORD_FILTER);
        File sourcedir = new File("/data/log/drill/sourcePatterns");
        if (filter != null && LogicalPlanUtil.needIncludes(filter, config, table)) {
          List<LogicalExpression> patterns = getPatterns(filter, table, config);

          String filterExpression = filter.get("expression").textValue();
//                    File sourcepttFile=new File(sourcedir.getAbsolutePath()+"_"+System.nanoTime());
//                    try {
//                        Writer writer=new FileWriter(sourcepttFile);
//                        for(int i=0;i<patterns.size();i++){
//                            writer.write(((ValueExpressions.QuotedString)patterns.get(i)).value+" ");
//                        }
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                        throw  new OptimizerException(e.getMessage());
//                    }
          HbaseScanPOP.RowkeyFilterEntry filterEntry = new HbaseScanPOP.RowkeyFilterEntry(Constants.FilterType.XaRowKeyPattern, patterns);
          filterEntries.add(filterEntry);
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
          long start = System.nanoTime() ;
          createHbaseScanEntry(selections, scan.getOutputReference(), entries);
          logger.info("Create scanEntry cost {} mills .",(System.nanoTime() - start)/1000_000);
          pop = new HbaseScanPOP(entries);
        } else if (SE_MYSQL.equals(storageEngine)) {
          JSONOptions root = scan.getSelection();
          if (root == null) {
            throw new OptimizerException("Selection is null");
          }
          JsonNode selection = root.getRoot(), projections;
          List<MysqlScanPOP.MysqlReadEntry> readEntries = Lists.newArrayList();
          if (selection instanceof ArrayNode) {
            for (JsonNode selectionNode : selection)
              readEntries.add(getMysqlEntry(selectionNode, config));
          } else
            readEntries.add(getMysqlEntry(selection, config));
          pop = new MysqlScanPOP(readEntries);

        } else {
          throw new OptimizerException("Unsupported storage engine - " + storageEngine);
        }
        operatorMap.put(scan, pop);
      }
      return pop;
    }

    private MysqlScanPOP.MysqlReadEntry getMysqlEntry(JsonNode selectionNode, DrillConfig config) throws OptimizerException {
      String tableName, filter = null;
      JsonNode projections;
      List<NamedExpression> projectionList = Lists.newArrayList();
      tableName = selectionNode.get(SELECTION_KEY_WORD_TABLE).textValue();
      if (selectionNode.get(SELECTION_KEY_WORD_FILTER) != null)
        filter = selectionNode.get(SELECTION_KEY_WORD_FILTER).textValue();
      projections = selectionNode.get(SELECTION_KEY_WORD_PROJECTIONS);
      ObjectMapper mapper = BasicOptimizer.this.config.getMapper();
      for (JsonNode projection : projections) {
        try {
          projectionList.add(mapper.readValue(projection.toString(), NamedExpression.class));
        } catch (IOException e) {
          throw new OptimizerException("Cannot parse projection : " + projection.toString());
        }
      }
      return new MysqlScanPOP.MysqlReadEntry(tableName, filter, projectionList);
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
        //logger.info(next);
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

    public List<LogicalExpression> getPatterns(JsonNode filter, String tableName, DrillConfig config) throws OptimizerException {
      LogicalExpression filterExpr = null;
      try {
        filterExpr = config.getMapper().readValue(filter.get("expression").traverse(), LogicalExpression.class);
      } catch (IOException e) {
        e.printStackTrace();
        throw new OptimizerException(e.getMessage());
      }
      return new ArrayList<>(getPatternsFromExpr(filterExpr, tableName, config));
    }

    private Set<LogicalExpression> getPatternsFromExpr(LogicalExpression filterExpr, String tableName, DrillConfig config) throws OptimizerException {
      if (!(filterExpr instanceof FunctionCall)) return null;

      try {
//                logger.info("enter get Patterns "+config.getMapper().writeValueAsString(filterExpr));
        Set<LogicalExpression> patterns = new HashSet<>();
        List<KeyPart> kps = null;
        kps = TableInfo.getRowKey(tableName, null);
        String projectId = tableName.contains("deu_") ? tableName.substring(4, tableName.length()) : tableName;
        projectId = projectId.contains("_deu") ? projectId.substring(0, projectId.length() - 4) : projectId;
        if (!((FunctionCall) filterExpr).getDefinition().getName().contains("or")) {
          Map<String, UnitFunc> fieldFunc = null;
//                    logger.info(" not or funccall "+config.getMapper().writeValueAsString(filterExpr));
          fieldFunc = parseFunctionCall((FunctionCall) filterExpr);
          Set<LogicalExpression> tmpPatterns = new HashSet<>(getPatternsFromColVals(fieldFunc, kps, projectId));
//                    if(tmpPatterns.size()!=0){
//                        logger.info("get patterns . patterns size is "+tmpPatterns.size());
//                        logger.info(((ValueExpressions.QuotedString)(new ArrayList<>(tmpPatterns)).get(0)).value);
//                        logger.info(((ValueExpressions.QuotedString)(new ArrayList<>(tmpPatterns)).get(tmpPatterns.size()-1)).value);
//                    }else {
//                        logger.info("do not get patterns from "+config.getMapper().writeValueAsString(filterExpr));
//                    }
          return tmpPatterns;
        } else {
//                    logger.info(" or funccall "+config.getMapper().writeValueAsString(filterExpr));
          for (LogicalExpression le : (FunctionCall) filterExpr) {
//                        logger.info("get patterns from "+config.getMapper().writeValueAsString(le));
            Set<LogicalExpression> tmpPatterns = getPatternsFromExpr(le, tableName, config);
//                        if(tmpPatterns.size()!=0){
//                        logger.info(((ValueExpressions.QuotedString)(new ArrayList<>(tmpPatterns)).get(0)).value);
//                        logger.info(((ValueExpressions.QuotedString)(new ArrayList<>(tmpPatterns)).get(tmpPatterns.size()-1)).value);
//                        }else {
//                            logger.info("do not get patterns from "+config.getMapper().writeValueAsString(le));
//                        }
            patterns.addAll(tmpPatterns);
          }
          return patterns;
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new OptimizerException(e.getMessage());
      }
    }

    private List<LogicalExpression> getPatternsFromColVals(Map<String, UnitFunc> fieldValueMap, List<KeyPart> kps, String projectId) throws OptimizerException {
//            logger.info("get patterns From Col Vals");
//            for(UnitFunc func:fieldValueMap.values()){
//                try {
//                    logger.info(config.getMapper().writeValueAsString(func.getFunc()));
//                } catch (JsonProcessingException e) {
//                    e.printStackTrace();
//                    throw new OptimizerException(e.getMessage());
//                }
//            }
      List<LogicalExpression> patterns = new ArrayList<>();
      List<KeyPart> workKps = kps;
      Deque<KeyPart> toWorkKps = new ArrayDeque<>(workKps);
      String event = "";
      String rkHead = "";
      loop:
      while (workKps.size() > 0) {
        for (KeyPart kp : workKps) {
          if (kp.getType() == KeyPart.Type.field) {
            String value;
            UnitFunc unitFunc = fieldValueMap.get(kp.getField().getName());
            if (unitFunc != null)
              value = unitFunc.getValue();
            else
              value = "*";
            if (kp.getField().getName().contains("event")) {
              event += value;
            } else {
              if (event.length() > 0)
                break loop;
              rkHead += value;
            }
            toWorkKps.removeFirst();
          } else if (kp.getType() == KeyPart.Type.constant) {
            if (kp.getConstant().equals("\\xFF")) break loop;
            if (event.length() != 0) {
              event += kp.getConstant();
            }
            toWorkKps.removeFirst();
          } else {
            toWorkKps.removeFirst();
            for (int i = kp.getOptionalGroup().size() - 1; i >= 0; i--) {
              toWorkKps.addFirst(kp.getOptionalGroup().get(i));
            }
            break;
          }
        }
        workKps = Arrays.asList(toWorkKps.toArray(new KeyPart[toWorkKps.size()]));
      }
      if (event.endsWith(".")) event = event.substring(0, event.length() - 1);
//            logger.info(" get Events from event "+projectId+" "+event);
      List<XEvent> events = null;
      try {
        events = XEventOperation.getInstance().getEvents(projectId, event);
      } catch (XEventException e) {
        e.printStackTrace();
        throw new OptimizerException(e.getMessage());
      } catch (MemCacheException e) {
        e.printStackTrace();
        throw new OptimizerException(e.getMessage());
      }
      List<String> resultEvents = new ArrayList<>();
      for (XEvent childEvent : events) {
        String resultEvent = "";
        for (int i = 0; i < childEvent.getEventArray().length; i++) {
          String levelEvent = childEvent.getEventArray()[i];
          if (levelEvent == null)
            break;
          resultEvent += levelEvent + ".";
        }
        resultEvents.add(resultEvent);
      }
      for (String resultEvent : resultEvents) {
        patterns.add(new ValueExpressions.QuotedString(rkHead + resultEvent + "\\xFF", ExpressionPosition.UNKNOWN));
      }
//            logger.info("out get patterns from col vals ");
      return patterns;
    }

    public Map<String, UnitFunc> parseFilterExpr(JsonNode origExpr, DrillConfig config) throws IOException {
      Map<String, UnitFunc> resultMap = new HashMap<>();
      LogicalExpression func = config.getMapper().readValue(origExpr.traverse(), LogicalExpression.class);
      return parseFunctionCall((FunctionCall) func);
    }

    public Map<String, UnitFunc> parseFunctionCall(FunctionCall func) {
      Map<String, UnitFunc> result = new HashMap<>();
      String field = null;
      UnitFunc value = null;
      for (LogicalExpression le : func) {
        if (le instanceof FunctionCall) {
          result.putAll(parseFunctionCall((FunctionCall) le));
        } else if (le instanceof SchemaPath) {
          field = ((SchemaPath) le).getPath().toString();
        } else if (le instanceof ValueExpressions.QuotedString) {
          value = new UnitFunc(func);
        }
      }
      if (field != null && value != null) {
        result.put(field, value);
      }
      return result;
    }

    public class UnitFunc {
      private String field;
      private String op;
      private String value;
      private FunctionCall func;

      public UnitFunc() {

      }

      public UnitFunc(FunctionCall func) {
        this.func = func;
        for (LogicalExpression le : func) {
          if (le instanceof SchemaPath) {
            field = ((SchemaPath) le).getPath().toString();
          } else if (le instanceof ValueExpressions.QuotedString) {
            value = ((ValueExpressions.QuotedString) le).value;
          }
        }
        op = func.getDefinition().getName();
      }

      public UnitFunc(String field, String op, String value) {
        this.field = field;
        this.op = op;
        this.value = value;
      }

      public String getField() {
        return field;
      }

      public String getOp() {
        return op;
      }

      public String getValue() {
        return value;
      }

      public FunctionCall getFunc() {
        return func;
      }
    }
  }
}
