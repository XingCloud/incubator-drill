package org.apache.drill.outer.manual;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.*;
import org.jgrapht.DirectedGraph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.SimpleGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.*;

public class PlanMerge {

  private final List<LogicalPlan> incoming;
  
  private List<LogicalPlan> merged;

  private Map<String, List<LogicalPlan>> sortedByProjectID;  

  public PlanMerge(List<LogicalPlan> plans) {
    this.incoming = plans;
    sortAndMerge();
  }

  private void sortAndMerge() {
    merged = new ArrayList<>();
    sortedByProjectID = new HashMap<>();
    /**
     * sort into sortedByProjectID, by projectID
     */
    for(LogicalPlan plan:incoming){
      String projectID = getProjectID(plan);
      List<LogicalPlan> projectPlans = sortedByProjectID.get(projectID);
      if(projectPlans == null){
        projectPlans = new ArrayList<>();
        sortedByProjectID.put(projectID, projectPlans);
      }
      projectPlans.add(plan);
    }
    for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
      String project = entry.getKey();
      List<LogicalPlan> plans = entry.getValue();

      ProjectMergeContext ctx = new ProjectMergeContext();
//      int mergeID = 1;
      for(LogicalPlan plan:plans){
        //init merging plan sets:初始状态每个plan自成一组
//        ctx.mergeID2plans.put(mergeID, new HashSet<LogicalPlan>(Arrays.asList(plan)));
//        ctx.plan2MergeID.put(plan, mergeID);
//        mergeID++;
        //找到有对相同table操作的scan
        Collection<SourceOperator> leaves = plan.getGraph().getLeaves();
        for(SourceOperator leaf:leaves){
          if(leaf instanceof Scan){
            Scan scan = (Scan) leaf;
            String tableName = getTableName(scan);
            ScanWithPlan swp = new ScanWithPlan(scan, plan, tableName);
            Set<ScanWithPlan> swps = ctx.tableName2Plans.get(tableName);
            if(swps == null){
              swps = new HashSet<>();
              ctx.tableName2Plans.put(tableName, swps);
            }
            swps.add(swp);
            Set<String> tableNames = ctx.plan2TableNames.get(plan);
            if(tableNames == null){
              tableNames = new HashSet<>();
              ctx.plan2TableNames.put(plan, tableNames);
            }
            tableNames.add(tableName);
          }
        }
      }
      
      //检查scan，标记合并
      for (Map.Entry<String, Set<ScanWithPlan>> entry2 : ctx.tableName2Plans.entrySet()) {
        String tableName = entry2.getKey();
        ScanWithPlan[] swps = new ScanWithPlan[entry2.getValue().size()];
        entry2.getValue().toArray(swps);
        for (int i = 0; i < swps.length; i++) {
          ScanWithPlan swpFrom = swps[i];
          for(int j = i+1; j < swps.length; j++){
            ScanWithPlan swpTo = swps[j];
            Mergeability<Scan> mergeability = mergeable(swpFrom.scan, swpTo.scan);
            if(mergeability != null){
              markMerge(swpFrom, swpTo, mergeability, ctx);
            }
          }
        }
      }
      //开始合并
      ctx.planInspector = new ConnectivityInspector<LogicalPlan, DefaultEdge>(ctx.mergePlanSets);
      List<Set<LogicalPlan>> mergeSets = ctx.planInspector.connectedSets();
      ctx.scanInspector = new ConnectivityInspector<Scan, DefaultEdge>(ctx.mergedScanSets);
      ctx.devidedScanSets = ctx.scanInspector.connectedSets();
      
      for (int i = 0; i < mergeSets.size(); i++) {
        Set<LogicalPlan> planSet = mergeSets.get(i);
        merged.add(doMergePlan(planSet, ctx));
      }
      ctx.close();
    }//for sortedByProjectID
  }

  /**
   * 合并两个plan的方法：
   * 
   * 如果两个scan完全一样，就合并。
   * 如果两个scan，其中一个的scan被另一个包含，就给他加一个filter，然后合并。
   * 非叶子节点：如果完全一样（子节点也一样），就合并。
   */
  private LogicalPlan doMergePlan(Set<LogicalPlan> plans, ProjectMergeContext projectCtx) {
    PlanProperties head = null;
    Map<String, StorageEngineConfig> se = null;
    PlanMergeContext planCtx = new PlanMergeContext();    
    for(LogicalPlan plan:plans){
      AdjacencyList<LogicalOperator> adjList = plan.getGraph().getAdjList();
      if(se == null){
        se = plan.getStorageEngines();
        head = plan.getProperties();
      }
      AdjacencyList<LogicalOperator> child2Parents = plan.getGraph().getAdjList().getReversedList();
      Collection<AdjacencyList<LogicalOperator>.Node> leaves = child2Parents.getInternalLeafNodes();
      Set<AdjacencyList<LogicalOperator>.Node> nextStepSet = new HashSet<>();
      //merge leaves first; then merge their parents
      for (AdjacencyList<LogicalOperator>.Node leaf : leaves) {
        LogicalOperator op = leaf.getNodeValue();
        if(!(op instanceof Scan)){
          continue;
        }
        Scan scan = (Scan) op;
        if(planCtx.mergedFrom2To.containsKey(scan)){
          //already merged
          continue;
        }
        //look for a already merged scan to which this scan can be merged
        Set<Scan> connectedScans = projectCtx.scanInspector.connectedSetOf(scan);
        Scan targetScan = null;
        if(connectedScans != null){
          for (Scan connectedScan : connectedScans) {
            if(connectedScan == scan){
              continue;
            }
            if(planCtx.mergedFrom2To.containsKey(connectedScan)){
              targetScan = connectedScan;
              break;
            }
          }
        }
        doMergeOperator(scan, targetScan, planCtx);
       
        lookForParentsAndSubstitute(leaf, child2Parents, nextStepSet, targetScan);
        
      }//for leaves
      //merge parents from leave, starting from nextStepSet
      for(;nextStepSet.size()>0;){
        Set<AdjacencyList<LogicalOperator>.Node> currentStepSet = nextStepSet;
        nextStepSet = new HashSet<>();
        //purge first, do merge for operators whose children have all already been merged
        for(Iterator<AdjacencyList<LogicalOperator>.Node> iterator = currentStepSet.iterator();iterator.hasNext();){
          AdjacencyList<LogicalOperator>.Node op = iterator.next();
          boolean childrenFullyMerged = true;
          for(LogicalOperator child:op.getNodeValue()){
            if(!planCtx.mergedFrom2To.containsKey(child)){
              childrenFullyMerged = false;
              break;
            }
          }
          if(!childrenFullyMerged){
            iterator.remove();
          }
        }
        for(AdjacencyList<LogicalOperator>.Node opNode:currentStepSet){
          LogicalOperator op = opNode.getNodeValue();
          //do not merge, simply add into result
          // TODO check carefully and merge
          
          doMergeOperator(op, null, planCtx);
          lookForParentsAndSubstitute(opNode, child2Parents, nextStepSet, null);          
        }
      }
    }//for plans
    
    //add union to all roots
    TopologicalOrderIterator<LogicalOperator, DefaultEdge> sorter = new TopologicalOrderIterator<LogicalOperator, DefaultEdge>(planCtx.mergedGraph);
    List<LogicalOperator> roots = new ArrayList<>();
    for(;sorter.hasNext();){
      LogicalOperator op = sorter.next();
      if(planCtx.mergedGraph.inDegreeOf(op)!=0){
        //not root now
        break;
      }
      roots.add(op);
    }
    if(roots.size()>1){
      Union union = new Union(roots.toArray(new LogicalOperator[roots.size()]), false);
      doMergeOperator(union, null, planCtx);
    }
    return new LogicalPlan(head, se, planCtx.mergeResult);  
  }

  private void doMergeOperator(LogicalOperator source, LogicalOperator target, PlanMergeContext planCtx) {
    if (target == null) {
      //no merge can be done
      planCtx.mergeResult.add(source);
      planCtx.mergedFrom2To.put(source, source);
      //new node added, change graph
      planCtx.mergedGraph.addVertex(source);
      for(LogicalOperator child:source){
        planCtx.mergedGraph.addEdge(source, child);
      }
    } else {
      //merge scan with targetScan
      planCtx.mergedFrom2To.put(source, target);
    }
  }

  private void lookForParentsAndSubstitute(AdjacencyList<LogicalOperator>.Node child, AdjacencyList<LogicalOperator> child2Parents, Collection<AdjacencyList<LogicalOperator>.Node> output
    , LogicalOperator substitionInParents) {
    List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(child);
    for (Edge<AdjacencyList<LogicalOperator>.Node> parentEdge : parentEdges) {
      //looking for all parents of scan, 
      // substitute scan with targetScan
      Edge<AdjacencyList<LogicalOperator>.Node> edge = (Edge<AdjacencyList<LogicalOperator>.Node>) parentEdge;
      AdjacencyList<LogicalOperator>.Node parentNode = edge.getTo();
      LogicalOperator parent = parentNode.getNodeValue();      
      //add parent for candidate for next round check
      output.add(parentNode);
      if(substitionInParents != null){
        substituteInParent(child.getNodeValue(), substitionInParents, parent);
      }
    }
  }

  private DirectedGraph<LogicalOperator, DefaultEdge> buildDirectedGraph(LogicalPlan plan) {
    return null;//adjList = plan.getGraph().getAdjList();
  }

  private void substituteInParent(LogicalOperator source, LogicalOperator target, LogicalOperator parent) {
    if (parent instanceof SingleInputOperator) {
      ((SingleInputOperator) parent).setInput(target);
    } else if (parent instanceof Join) {
      Join join = (Join) parent;
      if (join.getLeft().equals(source)) {
        join.setLeft(target);
      } else if (join.getRight().equals(source)) {
        join.setRight(target);
      }
    } else if (parent instanceof Union) {
      Union union = (Union) parent;
      LogicalOperator[] inputs = union.getInputs();
      for (int j = 0; j < inputs.length; j++) {
        LogicalOperator input = inputs[j];
        if (input.equals(source)) {
          inputs[j] = target;
          break;
        }
      }
    } else {
      throw new IllegalArgumentException("operator not supported!" + parent);
    }
  }

  private void markMerge(ScanWithPlan swpFrom, ScanWithPlan swpTo, Mergeability<Scan> mergeability, ProjectMergeContext ctx) {
    ctx.mergePlanSets.addVertex(swpFrom.plan);   
    ctx.mergePlanSets.addVertex(swpTo.plan);
    ctx.mergePlanSets.addEdge(swpFrom.plan, swpTo.plan);
    ctx.mergedScanSets.addVertex(mergeability.from);
    ctx.mergedScanSets.addVertex(mergeability.to);
    ctx.mergedScanSets.addEdge(mergeability.from, mergeability.to);
//    
//    int id1 = ctx.plan2MergeID.get(swpFrom);
//    int id2 = ctx.plan2MergeID.get(swpTo);
//    if(swpFrom == swpTo || id1 == id2){
//      //no need
//      return;
//    }
//    Set<LogicalPlan> set1 = ctx.mergeID2plans.get(id1);
//    Set<LogicalPlan> set2 = ctx.mergeID2plans.get(id2);
//    int toID, fromID;
//    Set<LogicalPlan> toSet, fromSet;
//    //to lower id
//    if(id1<id2){
//      toID = id1;
//      fromID = id2;
//      toSet = set1;
//      fromSet = set2;
//    }else{
//      toID = id2;
//      fromID = id1;
//      toSet = set2;
//      fromSet = set1;
//    }
//    toSet.addAll(fromSet);
//    ctx.mergeID2plans.remove(fromID);      
//    for(LogicalPlan fromPlan:fromSet){
//      ctx.plan2MergeID.put(fromPlan, toID);
//    }
//    fromSet.clear();
  }

  /**
   * 判断两个plan是否合并：
   * 如果两个plan有相同的scan，那就合并。
   * 如果两个plan，其中一个的scan范围包含另外一个，那就合并。
   * TODO 现在只合并一模一样的scan。
   * @param scan1
   * @param scan2
   * @return
   */
  private Mergeability<Scan> mergeable(Scan scan1, Scan scan2) {
    return equals(scan1, scan2);
  }

  private Mergeability<Scan> equals(Scan scan1, Scan scan2) {
    if(getTableName(scan1).equals(getTableName(scan2))
      && scan1.getSelection().equals(scan2.getSelection())){
      return new Mergeability<>(MergeType.same, scan1, scan2);
    }
    return null;
  }

  static class PlanMergeContext{
        //merge output
    List<LogicalOperator> mergeResult = new ArrayList<>();
    //all operators merged
    Map<LogicalOperator, LogicalOperator> mergedFrom2To = new HashMap<>();
    DirectedGraph<LogicalOperator, DefaultEdge> mergedGraph = new SimpleDirectedGraph<LogicalOperator, DefaultEdge>(DefaultEdge.class);
    
  }
  
  static class ProjectMergeContext{
    Map<String, Set<ScanWithPlan>> tableName2Plans = new HashMap<>();
    Map<LogicalPlan, Set<String>> plan2TableNames = new HashMap<>();
    UndirectedGraph<LogicalPlan, DefaultEdge> mergePlanSets = new SimpleGraph<LogicalPlan, DefaultEdge>(DefaultEdge.class);
    UndirectedGraph<Scan, DefaultEdge> mergedScanSets = new SimpleGraph<Scan, DefaultEdge>(DefaultEdge.class);
    ConnectivityInspector<Scan, DefaultEdge> scanInspector = null;
    ConnectivityInspector<LogicalPlan, DefaultEdge> planInspector = null;
    
    List<Set<Scan>> devidedScanSets = null;
    
//    Map<LogicalPlan, Integer> plan2MergeID = new HashMap<>();
//    Map<Integer, Set<LogicalPlan>> mergeID2plans = new HashMap<>(); 
    
    
    public void close() {
      DirectedGraph<Scan, DefaultEdge> g = new SimpleDirectedGraph<>(DefaultEdge.class);
      
      tableName2Plans.clear();
      tableName2Plans = null;
      plan2TableNames.clear();
      plan2TableNames = null;
      
//      plan2MergeID.clear();
//      plan2MergeID = null;
//      mergeID2plans.clear();
//      mergeID2plans = null;
    }
  }
  
  static class ScanWithPlan{
    Scan scan;
    LogicalPlan plan;
    String tableName;

    ScanWithPlan() {
    }

    ScanWithPlan(Scan scan, LogicalPlan plan, String tableName) {
      this.scan = scan;
      this.plan = plan;
      this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ScanWithPlan that = (ScanWithPlan) o;

      if (!plan.equals(that.plan)) return false;
      if (!scan.equals(that.scan)) return false;
      if (!tableName.equals(that.tableName)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = scan.hashCode();
      result = 31 * result + plan.hashCode();
      result = 31 * result + tableName.hashCode();
      return result;
    }
  }

  /**
   * LogicalPlan 必须带上plan属于的project的信息，才能被合并。
   * @param plan
   * @return
   */
  public static String getProjectID(LogicalPlan plan){
    return plan.getProperties().generator.info;
  }
  
  public static void setProjectID(LogicalPlan plan, String projectID){
    plan.getProperties().generator.info = projectID;
  }
  
  public static String getTableName(Scan scan){
    return scan.getSelection().getRoot().get("table").asText();    
  }

  public static List<LogicalPlan> sortAndMerge(List<LogicalPlan> plans) {
    return new PlanMerge(plans).getMerged();
  }

  public List<LogicalPlan> getMerged() {
    return merged;
  }
  
  static class Mergeability<T extends LogicalOperator>{
    
    MergeType mergeType;
    T from;
    T to;

    Mergeability() {
    }

    Mergeability(MergeType mergeType, T from, T to) {
      this.mergeType = mergeType;
      this.from = from;
      this.to = to;
    }
  }
  
  static enum MergeType{
    same, belongsto
  }
}
