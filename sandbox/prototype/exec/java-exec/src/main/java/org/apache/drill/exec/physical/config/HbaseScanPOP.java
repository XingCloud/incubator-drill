package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/23/13
 * Time: 12:27 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonTypeName("hbase-scan")
public class HbaseScanPOP extends AbstractScan<HbaseScanPOP.HbaseScanEntry> {
    @JsonCreator
    public HbaseScanPOP(@JsonProperty("entries") List<HbaseScanEntry> entries) {
        super(entries);
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {

    }

    @Override
    public Scan<?> getSpecificScan(int minorFragmentId) {
        return this;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        return Collections.emptyList();
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        return this;
    }

    public static class FilterEntry{


        private SchemaPath filterType;
        private List<LogicalExpression> filterExpressions;
        @JsonCreator
        public FilterEntry(@JsonProperty("type")FieldReference filterType,
                           @JsonProperty("exprs")List<LogicalExpression> filterExpressions) {
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
        private List<FilterEntry> filters;
        private List<NamedExpression>   projections;

        @JsonCreator
        public HbaseScanEntry(@JsonProperty("table") String tableName, @JsonProperty("startRowKey") String startRowKey,
                              @JsonProperty("endRowKey") String endRowKey, @JsonProperty("filters") List<FilterEntry> filters,
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

        public List<FilterEntry> getFilters() {
            return filters;
        }

        public List<NamedExpression> getProjections() {
            return projections;
        }
    }
}
