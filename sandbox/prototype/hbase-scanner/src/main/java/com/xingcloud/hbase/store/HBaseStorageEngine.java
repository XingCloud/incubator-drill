package com.xingcloud.hbase.store;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.xingcloud.hbase.reader.HBaseRecordReader;
import com.xingcloud.hbase.reader.HBaseScanPOP;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.QueryOptimizerRule;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordRecorder;

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
        throw new UnsupportedOperationException();
    }

    @Override
    public ListMultimap<ReadEntry, CoordinationProtos.DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
        return new HBaseRecordReader(context,(HBaseScanPOP.HBaseScanEntry)readEntry);
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
