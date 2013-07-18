package org.apache.drill.exec.store;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.HbaseUserScanPOP;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/11/13
 * Time: 12:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseStorageEngine extends AbstractStorageEngine {
    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public boolean supportsWrite() {
        return false;
    }

    @Override
    public List<QueryOptimizerRule> getOptimizerRules() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
        return null;
    }

    @Override
    public ListMultimap<ReadEntry, CoordinationProtos.DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
        if(readEntry instanceof HbaseScanPOP.HbaseScanEntry)
            return new HBaseRecordReader(context,(HbaseScanPOP.HbaseScanEntry)readEntry);
        else if(readEntry instanceof HbaseUserScanPOP.HbaseUserScanEntry)
            return new HBaseUserRecordReader(context,(HbaseUserScanPOP.HbaseUserScanEntry)readEntry);
        return null;
    }

    @Override
    public RecordRecorder getWriter(FragmentContext context, WriteEntry writeEntry) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multimap<CoordinationProtos.DrillbitEndpoint, ReadEntry> getEntryAssignments(List<CoordinationProtos.DrillbitEndpoint> assignments,
                                                                                        Collection<ReadEntry> entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Multimap<CoordinationProtos.DrillbitEndpoint, WriteEntry> getWriteAssignments(List<CoordinationProtos.DrillbitEndpoint> assignments,
                                                                                         Collection<ReadEntry> entries) {
        throw new UnsupportedOperationException();
    }

}
