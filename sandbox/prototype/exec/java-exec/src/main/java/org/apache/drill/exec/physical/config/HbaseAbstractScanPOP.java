package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.expression.LogicalExpression;
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
@JsonTypeName("hbase-abstract-scan")
public class HbaseAbstractScanPOP extends AbstractScan<HbaseAbstractScanPOP.HbaseAbstractScanEntry> {
    @JsonCreator
    public HbaseAbstractScanPOP(@JsonProperty("entries") List<HbaseAbstractScanEntry> entries) {
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

    public static class HbaseAbstractScanEntry implements ReadEntry {
        private String tableName;
        private byte[] startRowKey;
        private byte[] endRowKey;
        private List<LogicalExpression> filters;
        private List<NamedExpression>   projections;

        @JsonCreator
        public HbaseAbstractScanEntry(@JsonProperty("table")String tableName,@JsonProperty("startRowKey")byte[] startRowKey,
                                      @JsonProperty("endRowKey")byte[] endRowKey,@JsonProperty("filters")List<LogicalExpression> filters,
                                      @JsonProperty("projections")List<NamedExpression> projections){
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

        public byte[] getStartRowKey() {
            return startRowKey;
        }

        public byte[] getEndRowKey() {
            return endRowKey;
        }

        public List<LogicalExpression> getFilters() {
            return filters;
        }

        public List<NamedExpression> getProjections() {
            return projections;
        }
    }
}
