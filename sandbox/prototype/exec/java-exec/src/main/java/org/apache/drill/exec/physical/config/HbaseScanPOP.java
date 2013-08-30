package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/23/13
 * Time: 12:27 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonTypeName("hbase-scan")
public class HbaseScanPOP extends AbstractGroupScan implements SubScan, GroupScan{
  
  List<HbaseScanEntry> entries;
  
  @JsonCreator
  public HbaseScanPOP(@JsonProperty("entries") List<HbaseScanEntry> entries) {
    this.entries = entries;
  }

  @Override
  public OperatorCost getCost() {
    //todo actual cost
    return new OperatorCost(1, 1, 1, 1);
  }

  @Override
  public Size getSize() {
    //todo actual size;
    return new Size(1,1);
  }
  
  @JsonProperty("entries")
  public List<HbaseScanEntry> getEntries() {
    return entries;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        return this;
    }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return this;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return entries.size();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  public static class RowkeyFilterEntry {


        private SchemaPath filterType;
        private List<LogicalExpression> filterExpressions;
        @JsonCreator
        public RowkeyFilterEntry(@JsonProperty("type") FieldReference filterType,
                                 @JsonProperty("exprs") List<LogicalExpression> filterExpressions) {
            this.filterType = filterType;
            this.filterExpressions = filterExpressions;
        }

        public SchemaPath getFilterType() {
            return filterType;
        }

        public List<LogicalExpression> getFilterExpressions() {
            return filterExpressions;
        }


    }

    public static class HbaseScanEntry implements ReadEntry {
        private String tableName;
        private String startRowKey;
        private String endRowKey;
        private List<RowkeyFilterEntry> filters;
        private List<NamedExpression>   projections;

        @JsonCreator
        public HbaseScanEntry(@JsonProperty("table") String tableName, @JsonProperty("startRowKey") String startRowKey,
                              @JsonProperty("endRowKey") String endRowKey, @JsonProperty("filters") List<RowkeyFilterEntry> filters,
                              @JsonProperty("projections") List<NamedExpression> projections){
            this.tableName=tableName;
            this.startRowKey=startRowKey;
            this.endRowKey=endRowKey;
            this.filters=filters;
            this.projections=projections;
        }

        @Override
        public OperatorCost getCost() {
            return new OperatorCost(1, 2, 1, 1);
        }
        @Override
        public Size getSize() {
            return new Size(0,1);
        }

        public String getTableName() {
            return tableName;
        }

        public String getStartRowKey() {
            return startRowKey;
        }

        public String getEndRowKey() {
            return endRowKey;
        }

        public List<RowkeyFilterEntry> getFilters() {
            return filters;
        }

        public List<NamedExpression> getProjections() {
            return projections;
        }
    }
}
