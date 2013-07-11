package org.apache.drill.exec.physical.config;

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
 * User: witwolf
 * Date: 7/10/13
 * Time: 2:22 PM
 */
public class HbaseScanPOP extends AbstractScan<HbaseScanPOP.HbaseScanEntry>{

    public HbaseScanPOP(List<HbaseScanEntry> entries) {
        super(entries);

    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {

    }

    @Override
    public Scan<?> getSpecificScan(int minorFragmentId) {
        return null;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        return null;
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        return null;
    }
    static class HbaseScanEntry implements ReadEntry{

        private String project;
        private String startDate ;
        private String endDate ;
        private String eventPattern ;

        HbaseScanEntry(String project, String startDate, String endDate, String eventPattern) {
            this.project = project;
            this.startDate = startDate;
            this.endDate = endDate;
            this.eventPattern = eventPattern;
        }

        String getProject() {
            return project;
        }

        String getStartDate() {
            return startDate;
        }

        String getEndDate() {
            return endDate;
        }

        String getEventPattern() {
            return eventPattern;
        }

        @Override
        public OperatorCost getCost() {
            return null;
        }

        @Override
        public Size getSize() {
            return null;
        }
    }
}