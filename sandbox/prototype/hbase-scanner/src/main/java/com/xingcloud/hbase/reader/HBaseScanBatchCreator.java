package com.xingcloud.hbase.reader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/11/13
 * Time: 12:56 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseScanBatchCreator implements BatchCreator<HBaseScanPOP> {

    @Override
    public RecordBatch getBatch(FragmentContext context, HBaseScanPOP config, List<RecordBatch> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        List<HBaseScanPOP.HBaseScanEntry> entries = config.getReadEntries();
        List<RecordReader> readers = Lists.newArrayList();
        for(HBaseScanPOP.HBaseScanEntry e : entries){
            readers.add(new HBaseRecordReader(context, e));
        }
        return new ScanBatch(context, readers.iterator());
    }
}
