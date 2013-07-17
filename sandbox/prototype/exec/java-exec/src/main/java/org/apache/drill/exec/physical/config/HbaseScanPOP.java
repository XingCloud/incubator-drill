package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
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
 * User: witwolf
 * Date: 7/10/13
 * Time: 2:22 PM
 */

@JsonTypeName("hbase-scan")
public class HbaseScanPOP extends AbstractScan<HbaseScanPOP.HbaseScanEntry> {


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

    public static class HbaseScanEntry extends HbaseAbstractScanEntry{

        private String startDate;
        private String endDate;
        private String eventPattern;
        public HbaseScanEntry(@JsonProperty("pid") String project,
                                   @JsonProperty("startDate") String startDate,
                                   @JsonProperty("endDate") String endDate,
                                   @JsonProperty("event") String eventPattern) {
            super(project,"event");
            this.startDate = startDate;
            this.endDate = endDate;
            this.eventPattern = eventPattern;
        }
        public String getStartDate() {
            return startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public String getEventPattern() {
            return eventPattern;
        }
    }
    public static class HbaseUserScanEntry extends HbaseAbstractScanEntry{
        private String property;
        private String value;
        public HbaseUserScanEntry(@JsonProperty("pid") String project,
                                   @JsonProperty("propertry") String property,
                                   @JsonProperty("val")String value){
             super(project,"user");
             this.property=property;
             this.value=value;
        }
        public String getProperty(){
            return property;
        }
        public String getvalue(){
            return value;
        }
    }

    public static abstract class HbaseAbstractScanEntry implements ReadEntry {

        private String project;
        private String type;

        public HbaseAbstractScanEntry(String project,String type ) {
            this.project = project;
            this.type=type;
        }

        public String getProject() {
            return project;
        }
        public String getType(){
            return type;
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
