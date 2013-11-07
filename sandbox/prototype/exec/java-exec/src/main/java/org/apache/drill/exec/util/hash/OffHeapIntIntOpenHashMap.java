package org.apache.drill.exec.util.hash;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.DeadBuf;

import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: liuxiong
 * Date: 13-9-9
 * Time: 下午3:52
 */
public class OffHeapIntIntOpenHashMap implements Iterable<IntIntCursor> {

  private final BufferAllocator bufferAllocator;

  /**
   * Minimum capacity for the map.
   */
  public final static int MIN_CAPACITY = HashMapUtils.MIN_CAPACITY;

  /**
   * Default capacity.
   */
  public final static int DEFAULT_CAPACITY = HashMapUtils.DEFAULT_CAPACITY;

  /**
   * Default load factor.
   */
  public final static float DEFAULT_LOAD_FACTOR = HashMapUtils.DEFAULT_LOAD_FACTOR;

  /**
   * Hash-indexed array holding all keys.
   * interpret this bytebuf as an int array
   * @see #values
   */
  public ByteBuf keys;

  /**
   * Hash-indexed array holding all values associated to the keys
   * stored in {@link #keys}.
   * interpret this bytebuf as an int array.
   * @see #keys
   */
  public ByteBuf values;

  /**
   * Information if an entry (slot) in the {@link #values} table is allocated
   * or empty.
   * interpret this bytebuf as a boolean array
   * @see #assigned
   */
  public ByteBuf allocated;

  /**
   * Cached number of assigned slots in {@link #allocated}.
   */
  public int assigned;

  /**
   * The load factor for this map (fraction of allocated slots
   * before the buffers must be rehashed or reallocated).
   */
  public final float loadFactor;

  /**
   * Resize buffers when {@link #allocated} hits this value.
   */
  protected int resizeAt;

  /**
   * The most recent slot accessed in {@link #containsKey} (required for
   * {@link #lget}).
   *
   * @see #containsKey
   * @see #lget
   */
  protected int lastSlot;

  private AtomicInteger refCnt = new AtomicInteger(1);

  /**
   * Creates a hash map with the default capacity of {@value #DEFAULT_CAPACITY},
   * load factor of {@value #DEFAULT_LOAD_FACTOR}.
   *
   * <p>See class notes about hash distribution importance.</p>
   */
  public OffHeapIntIntOpenHashMap(BufferAllocator allocator){
    this(DEFAULT_CAPACITY, allocator);
  }

  /**
   * Creates a hash map with the given initial capacity, default load factor of
   * {@value #DEFAULT_LOAD_FACTOR}.
   *
   * <p>See class notes about hash distribution importance.</p>
   *
   * @param initialCapacity Initial capacity (greater than zero and automatically
   *            rounded to the next power of two).
   */
  public OffHeapIntIntOpenHashMap(int initialCapacity, BufferAllocator allocator){
    this(initialCapacity, DEFAULT_LOAD_FACTOR, allocator);
  }

  /**
   * Creates a hash map with the given initial capacity,
   * load factor.
   *
   * <p>See class notes about hash distribution importance.</p>
   *
   * @param initialCapacity Initial capacity (greater than zero and automatically
   *            rounded to the next power of two).
   *
   * @param loadFactor The load factor (greater than zero and smaller than 1).
   */
  public OffHeapIntIntOpenHashMap(int initialCapacity, float loadFactor, BufferAllocator allocator){
    initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

    assert initialCapacity > 0
        : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
    assert loadFactor > 0 && loadFactor <= 1
        : "Load factor must be between (0, 1].";

    this.loadFactor = loadFactor;
    this.bufferAllocator = allocator;

    // Allocate internal buffers for a given capacity.
    initBuffers(HashMapUtils.roundCapacity(initialCapacity));
  }

  private void initBuffers(int capacity){
    // capacity must be a power of two
    this.keys =  bufferAllocator.buffer(capacity << 2);//.order(ByteOrder.nativeOrder());
    this.values =  bufferAllocator.buffer(capacity << 2); //.order(ByteOrder.nativeOrder());
    this.allocated =  bufferAllocator.buffer(capacity);// .order(ByteOrder.nativeOrder());
    this.allocated.setZero(0, capacity);
    this.resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
  }


  /**
   * <p> Use the following snippet of code to check for key existence
   * first and then retrieve the value if it exists.</p>
   * <pre>
   * if (map.containsKey(key))
   *   value = map.lget();
   * </pre>
   */
  public int get(int key){
    final int mask = allocated.capacity() - 1;
    int slot = HashMapUtils.hash(key) & mask;
    final int wrappedAround = slot;

    while (allocated.getBoolean(slot)){
      if (key == keys.getInt(slot << 2)){
        return values.getInt(slot << 2);
      }

      slot = (slot + 1) & mask;
      if (slot == wrappedAround) break;
    }
    return 0;
  }

  /**
   * Returns the last value saved in a call to {@link #containsKey}.
   *
   * @see #containsKey
   */
  public int lget(){
    assert lastSlot >= 0 : "Call containsKey() first.";
    assert allocated.getBoolean(lastSlot): "Last call to exists did not have any associated value.";

    return values.getInt(lastSlot << 2);
  }

  /**
   * <p>Saves the associated value for fast access using {@link #lget} </p>
   * <pre>
   * if (map.containsKey(key))
   *   value = map.lget();
   * </pre>
   */
  public boolean containsKey(int key){
    final int mask = allocated.capacity() - 1;
    int slot = HashMapUtils.hash(key) & mask;
    final int wrappedAround = slot;

    while (allocated.getBoolean(slot)){
      if (key == keys.getInt(slot << 2)){
        lastSlot = slot;
        return true;
      }
      slot = (slot + 1) & mask;
      if (slot == wrappedAround) break;
    }
    lastSlot = -1;
    return false;
  }

  public int put(int key, int value){
    assert assigned < allocated.capacity();

    final int mask = allocated.capacity() - 1;
    int slot = HashMapUtils.hash(key) & mask;

    while (allocated.getBoolean(slot)){
      if (key == keys.getInt(slot << 2)){
        final int oldValue = values.getInt(slot << 2);
        values.setInt(slot << 2, value);
        return oldValue;
      }

      slot = (slot + 1) & mask;
    }

    // Check if we need to grow. If so, reallocate new data, fill in the last element and rehash.
    if (assigned == resizeAt) {
      expandAndPut(key, value, slot);
    } else {
      assigned++;
      allocated.setBoolean(slot, true);
      keys.setInt(slot << 2, key);
      values.setInt(slot << 2, value);
    }
    return 0;
  }

  /**
   * Expand the internal storage buffers (capacity) and rehash.
   */
  private void expandAndPut(int pendingKey, int pendingValue, int freeSlot){
    assert assigned == resizeAt;
    assert !allocated.getBoolean(freeSlot);

    // Try to allocate new buffers first. If we OOM, it'll be now without
    // leaving the data structure in an inconsistent state.
    int nextCapacity = HashMapUtils.nextCapacity(allocated.capacity());
    ByteBuf newKeys =  bufferAllocator.buffer(nextCapacity << 2);//.order(ByteOrder.nativeOrder());
    ByteBuf newValues =  bufferAllocator.buffer(nextCapacity << 2);//.order(ByteOrder.nativeOrder());
    ByteBuf newAllocated =  bufferAllocator.buffer(nextCapacity);//.order(ByteOrder.nativeOrder());
    // initialize to 0(false)
    newAllocated.setZero(0, newAllocated.capacity());
    int newResizeAt = Math.max(2, (int) Math.ceil(nextCapacity * loadFactor)) - 1;

    // We have succeeded at allocating new data so insert the pending key/value at
    // the free slot in the old arrays before rehashing.
    lastSlot = -1;
    assigned++;

    allocated.setBoolean(freeSlot, true);
    keys.setInt(freeSlot << 2, pendingKey);
    values.setInt(freeSlot << 2, pendingValue);

    // Rehash all stored keys into the new buffers.
    final int mask = newAllocated.capacity() - 1;
    for (int i = allocated.capacity(); --i >= 0;){
      if (allocated.getBoolean(i)){
        final int k = keys.getInt(i << 2);
        final int v = values.getInt(i << 2);

        int slot = HashMapUtils.hash(k) & mask;
        while (newAllocated.getBoolean(slot)){
          slot = (slot + 1) & mask;
        }

        newAllocated.setBoolean(slot, true);
        newKeys.setInt(slot << 2, k);
        newValues.setInt(slot << 2, v);
      }
    }

    assert allocated != newAllocated;
    assert keys != newKeys;
    assert values != newValues;

    releaseByteBuf(allocated, keys, values);
    this.allocated = newAllocated;
    this.keys = newKeys;
    this.values = newValues;
    this.resizeAt = newResizeAt;
  }

  private void releaseByteBuf(ByteBuf... bbs){
    for (ByteBuf bb : bbs){
      if(bb != DeadBuf.DEAD_BUFFER)
        bb.release();
    }
  }

  /**
   * clear the mappings
   * this does not mean the internal direct buffer is released.
   * to release the direct buffer, use {@link #release} method instead.
   */
  public void clear(){
    assigned = 0;
    releaseByteBuf(allocated, keys, values);
    initBuffers(DEFAULT_CAPACITY);
  }

  /**
   * release the internal direct buffer
   */
  public boolean release() {
    if (refCnt.get() == 0) {
      releaseByteBuf(allocated, keys, values);
      allocated = DeadBuf.DEAD_BUFFER;
      keys = DeadBuf.DEAD_BUFFER;
      values = DeadBuf.DEAD_BUFFER;
      return true;
    }
    return false;
  }

  public void retain(){
    refCnt.incrementAndGet();
  }


  public int size(){
    return assigned;
  }

  public boolean isEmpty(){
    return size() == 0;
  }

  @Override
  public int hashCode(){
    int h = 0;
    for(IntIntCursor cursor: this){
      h += HashMapUtils.hash(cursor.key) + HashMapUtils.hash(cursor.value);
    }
    return h;
  }

  @Override
  public String toString(){
    final StringBuilder buffer = new StringBuilder();
    buffer.append("[");

    boolean first = true;
    for(IntIntCursor cursor : this){
      if (!first) {
        buffer.append(", ");
      }

      buffer.append(cursor.key);
      buffer.append("=>");
      buffer.append(cursor.value);

      first = false;
    }

    buffer.append("]");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if(obj == null)
      return false;

    if(!(obj instanceof OffHeapIntIntOpenHashMap))
      return false;

    OffHeapIntIntOpenHashMap other = (OffHeapIntIntOpenHashMap)obj;
    if(other.size() != this.size())
      return false;

    for(IntIntCursor cursor: this){
      if(other.containsKey(cursor.key) && other.lget() == cursor.value){
        continue;
      }
      return false;
    }

    return true;
  }

  private final class EntryIterator implements Iterator<IntIntCursor>{

    private final IntIntCursor cursor;

    EntryIterator(){
      cursor = new IntIntCursor();
      cursor.index = -1;
    }

    private IntIntCursor fetch(){
      int i = cursor.index + 1;
      final int max = allocated.capacity();
      while (i < max && !allocated.getBoolean(i)){
        i++;
      }

      if (i == max)
        return null;

      cursor.index = i;
      cursor.key = keys.getInt(i << 2);
      cursor.value = values.getInt(i << 2);

      return cursor;
    }

    @Override
    public boolean hasNext() {
      return fetch() != null;
    }

    @Override
    public IntIntCursor next() {
      return cursor;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove operation currently not supported.");
    }
  }

  @Override
  public Iterator<IntIntCursor> iterator() {
    return new EntryIterator();
  }

}

