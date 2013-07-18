package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/17/13
 * Time: 7:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class HbaseUserScanPOP extends AbstractScan<HbaseUserScanPOP.HbaseUserScanEntry> {

    @JsonCreator
    public HbaseUserScanPOP(@JsonProperty("entries") List<HbaseUserScanEntry> entries) {
        super(entries);
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Scan<?> getSpecificScan(int minorFragmentId) {
        return this;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static class HbaseUserScanEntry implements ReadEntry {
        private String property;
        private String value;
        private String project;
        @JsonCreator
        public HbaseUserScanEntry(@JsonProperty("pid") String project,
                                  @JsonProperty("propertry") String property,
                                  @JsonProperty("val")String value){
            //super(project,"user");
            this.project=project;
            this.property=property;
            this.value=value;
        }
        @JsonCreator
        public HbaseUserScanEntry(@JsonProperty("pid") String project,
                                  @JsonProperty("propertry") String property){
            this.project=project;
            this.property=property;
            this.value=null;
        }
        public String getProperty(){
            return property;
        }
        public String getvalue(){
            return value;
        }
        public String getProject(){
            return project;
        }
        @Override
        public OperatorCost getCost() {
            return new OperatorCost(1, 2, 1, 1);
        }
        @Override
        public Size getSize() {
            return new Size(0,1);
        }
    }
}
