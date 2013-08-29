package org.apache.drill.exec.physical.impl.unionedscan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
  /*
   * 已经缓存的结果。
   * key 是 sortedIndex。
   */
  Map<Integer, List<CachedFrame>> cached = new HashMap<>();

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
  
  
  
  private int readerCurrentEntry;

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
        if(scanEntry.getEndRowKey().compareTo(sortedEntries.get(i+1).getStartRowKey())>0){
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
    schema = null;
    schemaChanged = true;
  }  
  
  private IterOutcome nextReaderOutput() {
    if(lastOutcome != null){
      IterOutcome ret = lastOutcome;
      lastOutcome = null;
      return ret;
    }
    try{
      recordCount = reader.next();
      logger.debug("reader.next():{}", recordCount);
    }catch(Exception e){
      logger.info("Reader.next() failed", e);
      releaseReaderAssets();
      return IterOutcome.STOP;
    }
    if(recordCount == 0){
      releaseReaderAssets();
      return IterOutcome.NONE;
    }
    if (schemaChanged) {
      schemaChanged = false;
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
    }    
  }

  private void releaseReaderAssets() {
    reader.cleanup();
  }

  /**
   * release cache and output variables of reader
   */
  private void releaseCache() {
    for (Map.Entry<Integer, List<CachedFrame>> entry : cached.entrySet()) {
      List<CachedFrame> value = entry.getValue();
      for (CachedFrame frame : value) {
        frame.close();
      }
      value.clear();
    }
    cached.clear();
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

  /**
   * 将reader 前进到指定的sortedEntry的起始处。
   * 之前的entry如果没有被输出，则将结果缓存下来。
   * @param sortedEntry
   * @return 如果成功前进到了指定的entry，则返回true；
   * 如果超过了指定的entry，则返回false。
   */
  boolean forwardReader2Before(int sortedEntry) {
    logger.debug("forwardReader2Before({})",sortedEntry);
    if(sortedEntry >= sortedEntries.size()){
      throw new IndexOutOfBoundsException("want to forward to the "+sortedEntry+"'th entry, but only have "+sortedEntries.size());
    }
    if(readerCurrentEntry == sortedEntry){
      return true;
    }
    List<CachedFrame> cache = null;
    while(true){
      IterOutcome outcome = nextReaderOutput();
      switch (outcome){
        case OK_NEW_SCHEMA:
        case OK:
          checkReaderOutputIntoNextEntry();
          cache = getCacheFor(readerCurrentEntry);
          if (readerCurrentEntry == sortedEntry) {
            //到达需要的这个entry
            stallOutcome(outcome);
            return true;
          } else if (readerCurrentEntry > sortedEntry) {
            //走过了这个entry
            stallOutcome(outcome);
            return false;
          } else {
            doCache(outcome, cache);
            continue;
          }
        default://NONE, STOP
          CachedFrame frame = new CachedFrame(schema,
            0, null,outcome, context);
          cache = getCacheFor(readerCurrentEntry);
          cache.add(frame);
          break;
      }//switch
    }
  }

  private List<CachedFrame> getCacheFor(int sortedEntry) {
    List<CachedFrame> cache = cached.get(sortedEntry);
        if(cache == null){
          cache = new ArrayList<>();
          cached.put(sortedEntry, cache);
        }
    return cache;
  }

  private void doCache(RecordBatch.IterOutcome outcome, List<CachedFrame> cache) {
    logger.debug("caching results for entry {} ...", readerCurrentEntry);
    int outRecordCount = recordCount;
    ArrayList<ValueVector> cachedVectors = new ArrayList<>();
    for (ValueVector v : vectors) {
      if(v.getField().getName().equals(UNION_MARKER_VECTOR_NAME)){
        v.clear();
      }else{
        TransferPair tp = v.getTransferPair();
        cachedVectors.add(tp.getTo());
        tp.transfer();
      }
    }


    CachedFrame frame = new CachedFrame(schema,
      outRecordCount, cachedVectors,
      outcome, context);
    cache.add(frame);
  }
  
  @Override
  public void kill() {
    releaseReaderAssets();
    releaseCache();
  }


  private class Mutator implements OutputMutator {
    private SchemaBuilder builder = BatchSchema.newBuilder();

    public void removeField(MaterializedField field) throws SchemaChangeException {
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

  static enum ScanMode {
    direct, cached
  }
  
  public class CachedFrame{
 
     /**
      * recorded iteration results
      */
     BatchSchema schema;
     private int recordCount;
     private List<ValueVector> vectors;
     private RecordBatch.IterOutcome outcome;
     private FragmentContext context;
 
     public CachedFrame(BatchSchema schema,
                                   int recordCount,
                                   List<ValueVector> vectors,
                                   RecordBatch.IterOutcome outcome,
                                   FragmentContext context) {
       this.schema = schema;
       this.recordCount = recordCount;
       this.vectors = vectors;
       this.outcome = outcome;
       this.context = context;
     }

    public BatchSchema getSchema() {
      return schema;
    }

    public int getRecordCount() {
      return recordCount;
    }

    public List<ValueVector> getVectors() {
      return vectors;
    }

    public IterOutcome getOutcome() {
      return outcome;
    }

    public FragmentContext getContext() {
      return context;
    }
    
    public void close() {
       //clear self
       if (vectors != null) {
         for (ValueVector v : vectors) {
           v.close();
         }
         this.vectors.clear();
         this.vectors = null;
       }
     }
  }
  
  public class UnionedScanSplitBatch implements RecordBatch{
  
    private final UnionedScanSplitPOP pop;
    private final FragmentContext context;
    
    ScanMode scanMode = null;
    public int currentEntryIndex = 0;
    public int currentCacheIndex = 0;
    public CachedFrame currentCachedData = null;

    public UnionedScanSplitBatch(FragmentContext context, UnionedScanSplitPOP config) {
      this.pop = config;
      this.context = context;
    }
  
    @Override
    public FragmentContext getContext() {
      return this.context;
    }
  
    @Override
    public BatchSchema getSchema() {
      switch (scanMode){
        case cached:
          return currentCachedData.getSchema();
        case direct:
          return schema;
        default:
          throw new IllegalArgumentException("can't recognize ScanMode:"+scanMode);
      }
    }
  
    @Override
    public int getRecordCount() {
      switch (scanMode){
        case cached:
          return currentCachedData.getRecordCount();
        case direct:
          return recordCount;
        default:
          throw new IllegalArgumentException("can't recognize ScanMode:"+scanMode);
      }
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
      switch (scanMode){
        case cached:
          return new VectorHolder(currentCachedData.getVectors()).getValueVector(path);
        case direct:
          return holder.getValueVector(path);          
        default:
          throw new IllegalArgumentException("can't recognize ScanMode:"+scanMode);
      }
    }
  
    @Override
    public <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz) {
      switch (scanMode){
        case cached:
          return new VectorHolder(currentCachedData.getVectors()).getValueVector(fieldId, clazz);
        case direct:
          return holder.getValueVector(fieldId, clazz);
        default:
          throw new IllegalArgumentException("can't recognize ScanMode:"+scanMode);
      }
    }
  
    @Override
    public IterOutcome next() {
      if(!splitsChecked){
        checkSplits();
        splitsChecked = true;
      }
      while (true) {
        //first next() to decide scanMode
        if (scanMode == null) {
          initScanMode();
        }
        if(scanMode == null){
          if(currentEntryIndex == pop.getEntries().length){
            //完成
            return IterOutcome.NONE;
          }
          //初始化scanMode失败
          throw new IllegalStateException("scanMode initialization failed!");
        }
        switch (scanMode) {
          case direct:
            IterOutcome outcome = nextReaderOutput();
            switch (outcome) {
              case NONE:
              case STOP:
                return outcome;
              case OK:
              case OK_NEW_SCHEMA:
                if (checkReaderOutputIntoNextEntry()) {
                  //本entry结束
                  stallOutcome(outcome);
                  reset2NextEntry();
                  continue;
                }
                //还在这个entry范围内
                if(logger.isDebugEnabled()){
                  Iterator<ValueVector> it;
                  for (it = passEntryID(vectors.iterator()); it.hasNext();) {
                    ValueVector vector = it.next();
                    logger.debug("vector on direct mode's next():{},{}...", vector.getAccessor().getValueCount(),vector.getAccessor().getObject(0));                    
                  }
                }
                return outcome;
            }
            break;
          case cached:
            if(currentCachedData != null){
              currentCachedData.close();
            }
            List<CachedFrame> cache = cached.get(currentSortedEntry());
            if(currentCacheIndex >=cache.size()){
              //eof, 本entry结束，go next
              reset2NextEntry();
              continue;
            }
            currentCachedData = cache.get(currentCacheIndex++);
            outcome = currentCachedData.getOutcome();
            if(outcome == IterOutcome.NONE || outcome == IterOutcome.STOP){
              currentCachedData.close();
            }
            return currentCachedData.getOutcome();
          default:
            throw new IllegalArgumentException("can't recognize ScanMode:"+scanMode);
        }
      }
    }

    private void reset2NextEntry(){
      scanMode = null;
      currentCacheIndex = 0;
      currentCachedData = null;
      currentEntryIndex++;
    }
    
    int currentSortedEntry(){
      if(currentEntryIndex >= pop.getEntries().length){
        return -1;
      }
      return original2sorted[pop.getEntries()[currentEntryIndex]];
    }

    private void initScanMode() {
      if(currentEntryIndex == pop.getEntries().length){
        scanMode = null;
        return;
      }
      int entryID = pop.getEntries()[currentEntryIndex];
      int sortedEntry = original2sorted[entryID];
      if(sortedEntry > readerCurrentEntry){
        //null, 还没扫到这个entry
        forwardReader2Before(sortedEntry);
        if(readerCurrentEntry == sortedEntry){
          scanMode = ScanMode.direct;
        }else if(readerCurrentEntry > sortedEntry){
          scanMode = ScanMode.direct;
        }else{
          throw new IllegalStateException("cannot forward to the "+sortedEntry+"'n entry!");
        }
      }else if(sortedEntry < readerCurrentEntry){
          this.scanMode = ScanMode.cached;
          if(cached.get(sortedEntry)==null){
            //已经扫过这个entry，但是没有结果，说明被别的split输出了结果
            throw new NullPointerException("cached output not found:"+entryID);
          }
          currentCacheIndex = 0;
      }else{//sortedEntry == readerCurrentEntry
          this.scanMode = ScanMode.direct;
      }
    }

    @Override
    public WritableBatch getWritableBatch() {
      return WritableBatch.get(this);
    }
  
    @Override
    public Iterator<ValueVector> iterator() {
      logger.debug("iterating vectors on mode:{}", scanMode);
      switch(scanMode){
        case direct:
          if(logger.isDebugEnabled()){
            ValueVector previous = null;
            for(Iterator<ValueVector> it = passEntryID(vectors.iterator());it.hasNext();){
              ValueVector vector = it.next();
              if(vector == previous){
                logger.warn("previous vector same as this!{}", vector.getField());
              }
              previous = vector;
              logger.debug("vector on direct mode's iterator():{},{}...", vector.getAccessor().getValueCount(),vector.getAccessor().getObject(0));
              try {
                Field dataField = vector.getClass().getSuperclass().getDeclaredField("data");
                dataField.setAccessible(true);
                Object data = dataField.get(vector);
                logger.debug("data:{}",data.getClass());
              } catch (NoSuchFieldException e) {
                e.printStackTrace();  //e:
              } catch (IllegalAccessException e) {
                e.printStackTrace();  //e:
              }
            }
          }
          return passEntryID(vectors.iterator());
        case cached:
          return currentCachedData.getVectors().iterator();
        default:
          throw new IllegalArgumentException("can't recognize scanMode:"+scanMode);
      }
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

  /**
   * 检查是否上次reader输出的内容，已经进入了下一个entry的范围;
   * 如果进入了下一个entry，则更新readerCurrentEntry以及cache。
   * 同时，将vector里面的entrymark删掉。
   * @return
   */
  private boolean checkReaderOutputIntoNextEntry() {
    int scanningEntry = getEntryMark();
    logger.debug("entry mark for this output:{}", scanningEntry);
    if(scanningEntry == -1){
      throw new IllegalStateException("cannot find entry mark!");
    }
    removeEntryMark();
    boolean intoNext = false;
    if(scanningEntry > readerCurrentEntry){
      intoNext = true;
      markScanNext();
      for(;scanningEntry > readerCurrentEntry;){
        //可能中间有无结果，直接被跳过的entry
        List<CachedFrame> cache = getCacheFor(readerCurrentEntry);
        cache.add(new CachedFrame(null, 0, null, IterOutcome.NONE, context));
        markScanNext();              
      }            
    }
    logger.debug("checkReaderOutputIntoNextEntry():{}", intoNext);
    return intoNext;
  }

  /**
   * 标记reader开始扫描下一个entry
   */
  private void markScanNext() {
    readerCurrentEntry++;
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
