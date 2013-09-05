package org.apache.drill.exec.vector;

import com.xingcloud.xa.encoding.Int64RemoveLeadingZeroByte;
import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-2
 * Time: 下午2:30
 * To change this template use File | Settings | File Templates.
 */
public class VarBigIntVector extends BaseDataValueVector implements VariableWidthVector{
  static final Logger logger = LoggerFactory.getLogger(VarBigIntVector.class);

  private final SmallIntVector offsetVector;
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();


  public VarBigIntVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.offsetVector = new SmallIntVector(null, allocator);
  }

  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    clear();
    assert totalBytes >= 0;
    data = allocator.buffer(totalBytes);
    data.readerIndex(0);
    offsetVector.allocateNew(valueCount+1);
    offsetVector.getMutator().set(0, 0);
  }

  @Override
  public int getByteCapacity() {
    return data.capacity();
  }

  @Override
  public int load(int dataBytes, int valueCount, ByteBuf buf) {
    this.valueCount = valueCount;
    int loaded = offsetVector.load(valueCount+1, buf);
    data = buf.slice(loaded, dataBytes - loaded);
    data.retain();
    return dataBytes;
  }



  @Override
  public TransferPair getTransferPair() {
    return new TransferImpl();
  }

  @Override
  public int getValueCapacity() {
    return offsetVector.getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void load(UserBitShared.FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getBufferLength(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public final class Accessor extends BaseValueVector.BaseAccessor{

    public long get(int index) {
      assert index >= 0;
      int startIdx = offsetVector.getAccessor().get(index);
      int length = 0;
      if (index == offsetVector.getValueCapacity()-1) {
        //The last one
        length = data.capacity() - startIdx;
      } else {
        length = offsetVector.getAccessor().get(index + 1) - startIdx;
      }

      assert length >= 0;
      byte[] dst = new byte[length];
      data.getBytes(startIdx, dst, 0, length);
      long value = Int64RemoveLeadingZeroByte.decode(dst);
      return value;
    }

    public void get(int index, VarBigIntHolder holder){
      holder.start = offsetVector.getAccessor().get(index);
      holder.end = offsetVector.getAccessor().get(index + 1);
      holder.buffer = data;
    }


    public Object getObject(int index) {
      return get(index);
    }

    public int getValueCount() {
      return valueCount;
    }

    public SmallIntVector getOffsetVector(){
      return offsetVector;
    }
  }

  public final class Mutator extends BaseValueVector.BaseMutator{

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param value   value to set
     */
    public void set(int index, long value) {
      assert index >= 0;
      int currentOffset = offsetVector.getAccessor().get(index);
      byte[] encodedBytes = Int64RemoveLeadingZeroByte.encode(value);
      if (data.capacity() < currentOffset + encodedBytes.length) {
        realloc(index, currentOffset, encodedBytes.length);
      }
      offsetVector.getMutator().set(index + 1, currentOffset + encodedBytes.length);
      data.setBytes(currentOffset, encodedBytes);
    }

    public void set(int index, VarBigIntHolder holder){
      int length = holder.end - holder.start;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, holder.buffer, holder.start, length);
    }

    void set(int index, NullableVarCharHolder holder){
      int length = holder.end - holder.start;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, holder.buffer, holder.start, length);
    }

    public void setValueCount(int valueCount) {
      VarBigIntVector.this.valueCount = valueCount;
      data.writerIndex(offsetVector.getAccessor().get(valueCount));
      offsetVector.getMutator().setValueCount(valueCount+1);
    }

    @Override
    public void generateTestData() {
      setValueCount(getValueCapacity());
      boolean even = true;
      for(int i =0; i < valueCount; i++, even = !even){
        if(even){
          set(i, Long.MIN_VALUE);
        }else{
          set(i, Long.MAX_VALUE);
        }
      }
    }

    @Override
    public void setObject(int index, Object obj) {
      set(index, (Long)obj);
    }

    @Override
    public void transferTo(ValueVector target, boolean needClear) {
      VarBigIntVector.this.transferTo((VarBigIntVector)target, needClear);
    }


  }

  private class TransferImpl implements TransferPair{
    VarBigIntVector to;

    public TransferImpl(){
      this.to = new VarBigIntVector(getField(), allocator);
    }

    public VarBigIntVector getTo(){
      return to;
    }

    public void transfer(){
      transferTo(to, true);
    }

    public void mirror(){
      transferTo(to, false);
    }
  }

  public void transferTo(VarBigIntVector target, boolean needClear){
    this.offsetVector.transferTo(target.offsetVector, needClear);
    target.data = data;
    target.data.retain();
    target.valueCount = valueCount;
    if(needClear){
      clear();
    }
  }

  private int getNewSize(int setCount, int setOffset, int length){
    int averageSize = (int) Math.ceil((setOffset + length + 1) / (setCount + 1.0) ) ;
    return getValueCapacity() * averageSize ;
  }

  private void realloc(int setCount, int setOffset, int length){
    int newLength = getNewSize(setCount, setOffset, length);
    ByteBuf newBuf = allocator.buffer(newLength);
    newBuf.readerIndex(0);
    data.writerIndex(setOffset);
    newBuf.setBytes(0, data,setOffset);
    data.release() ;
    data = newBuf;
  }




}
