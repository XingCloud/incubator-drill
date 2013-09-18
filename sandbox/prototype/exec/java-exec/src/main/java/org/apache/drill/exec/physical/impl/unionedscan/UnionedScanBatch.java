package org.apache.drill.exec.physical.impl.unionedscan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xingcloud.meta.ByteUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HbaseScanPOP;
import org.apache.drill.exec.physical.config.UnionedScanSplitPOP;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.VectorHolder;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.*;

public class UnionedScanBatch implements RecordBatch {

  public static final Logger logger = LoggerFactory.getLogger(UnionedScanBatch.class);

  /**
   * 这一个Vector，在MultiEntry reader 和 UnionedScan 之间使用。
   * 所以UnionedScan往上，schema和vectors都屏蔽掉这一列。
   */
  public static final String UNION_MARKER_VECTOR_NAME = "unioned_scan_entry_id";
  
  /**
   * entry id 就是entry在original entries 里面的序号。
   * 将original Entry 重新排序，形成真正的entry
   */
  private final List<HbaseScanPOP.HbaseScanEntry> originalEntries;
  private List<HbaseScanPOP.HbaseScanEntry> sortedEntries;
  int[] original2sorted;
  private List<UnionedScanSplitBatch> splits = new ArrayList<>();
  private boolean splitsChecked = false;
  private FragmentContext context;
  
  IterOutcome lastOutcome = null;
  
  /**
   * 为reader 输出准备的变量
   */
  private MultiEntryHBaseRecordReader reader = null;
  
  final List<ValueVector> vectors = Lists.newLinkedList();
  final Map<MaterializedField, ValueVector> fieldVectorMap = Maps.newHashMap();
  private VectorHolder holder = new VectorHolder(vectors);
  private BatchSchema schema;
  private int recordCount;
  private boolean schemaChanged = true;

  private final Mutator mutator = new Mutator();
  
  private int lastReaderEntry = -1;

  public UnionedScanBatch(FragmentContext context, List<HbaseScanPOP.HbaseScanEntry> readEntries) throws ExecutionSetupException {
    this.context = context;
    this.originalEntries  = readEntries;
    sortEntries();
    checkEntryOverlap();
    initReader();
  }

  /**
   * 保证所有entry的startKey, endKey范围没有重叠的；
   * @throws ExecutionSetupException
   */
  private void checkEntryOverlap() throws ExecutionSetupException {
    for (int i = 0; i < sortedEntries.size(); i++) {
      HbaseScanPOP.HbaseScanEntry scanEntry = sortedEntries.get(i);
      if(i<sortedEntries.size()-1){
        byte[] thisEnd = ByteUtils.toBytesBinary(scanEntry.getEndRowKey());
        byte[] nextStart = ByteUtils.toBytesBinary(sortedEntries.get(i+1).getStartRowKey());
        if(ByteUtils.compareBytes(thisEnd, nextStart) > 0){
          throw new ExecutionSetupException("entry rowkey overlap: "+i+"'th endKey:"+scanEntry.getEndRowKey()+" vs next startKey:"+sortedEntries.get(i+1).getStartRowKey());
        }
      }
    }
  }

  private void initReader() throws ExecutionSetupException {
    this.reader = new MultiEntryHBaseRecordReader(context, sortedEntries.toArray(new HbaseScanPOP.HbaseScanEntry[sortedEntries.size()]));
    reader.setup(mutator);
  }

  private void sortEntries() {
    Map<HbaseScanPOP.HbaseScanEntry, Integer> entry2OriginalId = new HashMap<>();
    for (int i = 0; i < originalEntries.size(); i++) {
      HbaseScanPOP.HbaseScanEntry scanEntry = originalEntries.get(i);
      entry2OriginalId.put(scanEntry, i);
    }
    HbaseScanPOP.HbaseScanEntry[] sorted = 
    new ArrayList<>(originalEntries).toArray(new HbaseScanPOP.HbaseScanEntry[originalEntries.size()]);
    Arrays.sort(sorted, new Comparator<HbaseScanPOP.HbaseScanEntry>() {
      @Override
      public int compare(HbaseScanPOP.HbaseScanEntry o1, HbaseScanPOP.HbaseScanEntry o2) {
        return o1.getStartRowKey().compareTo(o2.getStartRowKey());
      }
    });
    int[] original2sorted = new int[sorted.length];
    for (int i = 0; i < sorted.length; i++) {
      HbaseScanPOP.HbaseScanEntry hbaseScanEntry = sorted[i];
      original2sorted[entry2OriginalId.get(hbaseScanEntry)] = i;
    }
    this.original2sorted = original2sorted;
    this.sortedEntries = Arrays.asList(sorted);
  }

  public RecordBatch createSplitBatch(FragmentContext context, UnionedScanSplitPOP config) {
    UnionedScanSplitBatch split = new UnionedScanSplitBatch(context, config);
    splits.add(split);
    return split;
  }
  
  private void schemaChanged() {
    //schema = null;
    schemaChanged = true;
  }  
  
  private IterOutcome nextReaderOutput() {
    if(reader == null){
      return IterOutcome.NONE;
    }
    if(lastOutcome != null){
      IterOutcome ret = lastOutcome;
      lastOutcome = null;
      if(ret == null){
        throw new NullPointerException("lastOutcome becomes null!");
      }
      return ret;
    }
    try{
      recordCount = reader.next();
    }catch(Exception e){
      logger.info("Reader.next() failed", e);
      releaseReaderAssets();
      return IterOutcome.STOP;
    }
    if(recordCount == 0){
      releaseReaderAssets();
      return IterOutcome.NONE;
    }
    lastReaderEntry = getEntryMark();
    removeEntryMark();
    if (schemaChanged) {
      schemaChanged = false;
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
    }    
  }

 
  private void releaseReaderAssets() {
    if(reader != null){
      reader.cleanup();
      reader = null;
    }
  }

  /**
   * release cache and output variables of reader
   */
  private void releaseCache() {
    if(vectors!=null){
      for (ValueVector valueVector : vectors) {
        valueVector.close();
      }
      vectors.clear();      
    }
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }
  
  @Override
  public void kill() {
    releaseReaderAssets();
    releaseCache();
  }

  public int[] getOriginalID2sorted() {
    return original2sorted;
  }

  private class Mutator implements OutputMutator {
    private SchemaBuilder builder = BatchSchema.newBuilder();

    public void removeField(MaterializedField field) throws SchemaChangeException {
      //for test
      //if(!fieldVectorMap.containsKey(field))
      //    return;
      schemaChanged();
      ValueVector vector = fieldVectorMap.remove(field);
      if (vector == null) throw new SchemaChangeException("Failure attempting to remove an unknown field.");
      vectors.remove(vector);
      vector.close();
      if(!vector.getField().getName().equals(UNION_MARKER_VECTOR_NAME)){
        builder.removeField(vector.getField());
      }
    }

    public void addField(ValueVector vector) {
      vectors.add(vector);
      fieldVectorMap.put(vector.getField(), vector);
      if(!vector.getField().getName().equals(UNION_MARKER_VECTOR_NAME)){
        builder.addField(vector.getField());
      }
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
      UnionedScanBatch.this.schema = this.builder.build();
      UnionedScanBatch.this.schemaChanged = true;
    }

  }
  
  public class UnionedScanSplitBatch implements RecordBatch{
  
    private final UnionedScanSplitPOP pop;
    private final FragmentContext context;
    
    public int currentEntryIndex = 0;

    public int mySortedEntry = 0;
    public UnionedScanSplitBatch(FragmentContext context, UnionedScanSplitPOP config) {
      this.pop = config;
      this.context = context;
      if(this.pop.getEntries().length != 1){
        throw new IllegalArgumentException("only support single entry for ScanSplit!");
      }
      mySortedEntry = original2sorted[this.pop.getEntries()[0]];
    }

    public UnionedScanSplitPOP getPop() {
      return pop;
    }

    @Override
    public FragmentContext getContext() {
      return this.context;
    }
  
    @Override
    public BatchSchema getSchema() {
      /*if(lastReaderEntry != mySortedEntry){
        throw new IllegalStateException("last reader entry:"+lastReaderEntry+", current split index:"+original2sorted[pop.getEntries()[0]]);
      }*/
      return schema;
    }
  
    @Override
    public int getRecordCount() {
      if(lastReaderEntry != mySortedEntry){
        throw new IllegalStateException("last reader entry:"+lastReaderEntry+", current split index:"+original2sorted[pop.getEntries()[0]]);
      }
      return recordCount;
      
    }
  
    @Override
    public void kill() {
      releaseReaderAssets();
      releaseCache();
    }
  
    @Override
    public SelectionVector2 getSelectionVector2() {
      throw new UnsupportedOperationException();      
    }
  
    @Override
    public SelectionVector4 getSelectionVector4() {
      throw new UnsupportedOperationException();      
    }
  
    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      if(lastReaderEntry != mySortedEntry){
        throw new IllegalStateException("last reader entry:"+lastReaderEntry+", current split index:"+original2sorted[pop.getEntries()[0]]);
      }
      return holder.getValueVector(path);          
    }
  
    @Override
    public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
      if(lastReaderEntry != mySortedEntry){
        throw new IllegalStateException("last reader entry:"+lastReaderEntry+", current split index:"+original2sorted[pop.getEntries()[0]]);
      }
      return holder.getValueVector(fieldId, clazz);
    }
  
    @Override
    public IterOutcome next() {
      if(!splitsChecked){
        checkSplits();
        splitsChecked = true;
      }
      IterOutcome outcome = nextReaderOutput();
      if(lastReaderEntry == -1){
        return IterOutcome.NONE;
      }
      if(outcome == null){
        throw new NullPointerException("null outcome received from nextReaderOutput()!");
      }
      if(lastReaderEntry > mySortedEntry){
        //reader run passed this split
        stallOutcome(outcome);
        return IterOutcome.NONE;
      }
      if(lastReaderEntry < mySortedEntry){
        if(outcome == IterOutcome.NONE)
          return IterOutcome.NONE;
        //cannot reach this point!
        throw new IllegalStateException("reader:"+lastReaderEntry+" is behind this split:"+mySortedEntry);
      }
      return outcome;
    }

    @Override
    public WritableBatch getWritableBatch() {
      return WritableBatch.get(this);
    }
  
    @Override
    public Iterator<ValueVector> iterator() {
      if(lastReaderEntry != mySortedEntry){
        throw new IllegalStateException("last reader entry:"+lastReaderEntry+", current split index:"+original2sorted[pop.getEntries()[0]]);
      }
      return passEntryID(vectors.iterator());
    }

    public UnionedScanBatch getIncoming(){
      return UnionedScanBatch.this;
    }
  }

  private Iterator<ValueVector> passEntryID(Iterator<ValueVector> iterator) {
    return new PassEntryID(iterator);
  }
  
  static class PassEntryID implements Iterator<ValueVector>{

    Iterator<ValueVector> incoming;

    ValueVector nextV;
    
    PassEntryID(Iterator<ValueVector> incoming) {
      this.incoming = incoming;
      nextNoEntryID();
    }

    private void nextNoEntryID() {
      if(incoming.hasNext()){
        nextV = incoming.next();
        while(nextV.getField().getName().equals(UNION_MARKER_VECTOR_NAME)&& incoming.hasNext()){
          nextV = incoming.next();
        }
        if(nextV.getField().getName().equals(UNION_MARKER_VECTOR_NAME)){
          nextV = null;
        }
      }else{
        nextV = null;
      }
    }

    @Override
    public boolean hasNext() {
      return nextV != null;
    }

    @Override
    public ValueVector next() {
      ValueVector tmp = nextV;
      nextNoEntryID();
      return tmp;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("cannot remove !");
    }
  }

  /**
   * 检查：所有的split的entry之间，没有重叠；所有的split的entry加起来，没有遗漏
   */
  private void checkSplits() {
    HashSet<Integer> entrySet = new HashSet<>();
    for (int i = 0; i < splits.size(); i++) {
      UnionedScanSplitBatch splitBatch = splits.get(i);
      int[] es = splitBatch.pop.getEntries();
      for (int j = 0; j < es.length; j++) {
        int e = es[j];
        if(entrySet.contains(e)){
          throw new IllegalStateException("entry: "+e+" duplicated in splits!");
        }
        entrySet.add(e);
      }
    }
    for (int i = 0; i < sortedEntries.size(); i++) {
      if(!entrySet.contains(i)){
        throw new IllegalStateException("entry: "+i+" belongs to no split!");
      }
    }
    splitsChecked = true;
  }


  private int getEntryMark() {
    for(ValueVector v:vectors){
      if(v.getField().getName().equals(UNION_MARKER_VECTOR_NAME)){
        return (Integer)v.getAccessor().getObject(0);        
      }
    }
    return -1;//impossible
  }
  
  /**
   * 清除ValueVector和schema中，内部使用的表示哪个entry的标记valuevector
   */
  private void removeEntryMark() {
    for(ValueVector v:vectors){
      if(v.getField().getName().equals(UNION_MARKER_VECTOR_NAME)){
        try {
          v.clear();
          break;
        } catch (Exception e) {
          logger.warn("removeEntryMark failed", e);
          throw new IllegalArgumentException("cannot find union marker vector!");
        }
      }
    }
  }

  /**
   * 将outcome暂存入lastOutcome，下次再使用
   * @param outcome
   */
  private void stallOutcome(IterOutcome outcome) {
    lastOutcome = outcome;
  }
 
  @Override
   public BatchSchema getSchema() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public int getRecordCount() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");    
   }
 
   @Override
   public SelectionVector2 getSelectionVector2() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public SelectionVector4 getSelectionVector4() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public TypedFieldId getValueVectorId(SchemaPath path) {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public IterOutcome next() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public WritableBatch getWritableBatch() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }
 
   @Override
   public Iterator<ValueVector> iterator() {
     throw new UnsupportedOperationException("UnionedScanBatch should not be called like other batches");
   }

}
