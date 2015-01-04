package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
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
 * 李强
 */

@JsonTypeName("user-scan")
public class UserScanPOP extends AbstractScan<UserScanPOP.UserReadEntry> {


  @JsonCreator
  public UserScanPOP(@JsonProperty("entries") List<UserReadEntry> entries) {
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

  public static class UserReadEntry implements ReadEntry {

    private String tableName ;
    private String filter ;
    private List<NamedExpression> projections ;

    public UserReadEntry(@JsonProperty("table")String tableName,
                          @JsonProperty("filter") String filter,
                          @JsonProperty("projections") List<NamedExpression> projections) {
      this.tableName = tableName;
      this.filter = filter;
      this.projections = projections;
    }

    @Override
    public OperatorCost getCost() {
      return new OperatorCost(1,2,1,1);
    }

    @Override
    public Size getSize() {
      return new Size(0,1);
    }

    public String getTableName() {
      return tableName;
    }

    public String getFilter() {
      return filter;
    }

    public List<NamedExpression> getProjections() {
      return projections;
    }
  }
}
