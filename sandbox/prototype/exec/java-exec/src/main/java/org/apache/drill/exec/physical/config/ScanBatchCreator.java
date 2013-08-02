package org.apache.drill.exec.physical.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.HBaseRecordReader;
import org.apache.drill.exec.store.RecordReader;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/12/13
 * Time: 3:30 PM
 */
public class ScanBatchCreator implements BatchCreator<AbstractScan> {

    @Override
    public RecordBatch getBatch(FragmentContext context, AbstractScan config, List<RecordBatch> children) throws ExecutionSetupException {

        Preconditions.checkArgument(children.isEmpty());
        List<RecordReader> readers = Lists.newArrayList();
        if (config instanceof MockScanPOP) {
            List<MockScanPOP.MockScanEntry> entries = config.getReadEntries();
            for (MockScanPOP.MockScanEntry e : entries) {
                readers.add(new MockRecordReader(context, e));
            }
        }else if(config instanceof HbaseScanPOP){
            List<HbaseScanPOP.HbaseScanEntry> readEntries=config.getReadEntries();
            for(HbaseScanPOP.HbaseScanEntry entry: readEntries){
                readers.add(new HBaseRecordReader(context,entry));
            }
        }
        return new ScanBatch(context, readers.iterator());
    }


}
