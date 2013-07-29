package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
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

    public static class HbaseScanEntry implements ReadEntry {

        private String startDate;
        private String endDate;
        private String eventPattern;
        private String project;
        @JsonCreator
        public HbaseScanEntry(@JsonProperty("pid") String project,
                                   @JsonProperty("startDate") String startDate,
                                   @JsonProperty("endDate") String endDate,
                                   @JsonProperty("event") String eventPattern) {
            this.project=project;
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
