package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 8/20/13
 * Time: 2:14 PM
 */
public class WrappedByteBuf extends ByteBuf {

  static final Logger logger = LoggerFactory.getLogger(WrappedByteBuf.class);

  private ByteBuf byteBuf;
  private BufferAllocator allocator;
  private int refCount;

  public WrappedByteBuf(ByteBuf byteBuf, BufferAllocator allocator) {
    this.byteBuf = byteBuf;
    this.allocator = allocator;
    this.refCount = 1;
  }

  @Override
  public int capacity() {
    return byteBuf.capacity();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    return byteBuf.capacity(newCapacity);
  }

  @Override
  public int maxCapacity() {
    return byteBuf.maxCapacity();
  }

  @Override
  public ByteBufAllocator alloc() {
    return byteBuf.alloc();
  }

  @Override
  public ByteOrder order() {
    return byteBuf.order();
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    return byteBuf.order(endianness);
  }

  @Override
  public ByteBuf unwrap() {
    return byteBuf.unwrap();
  }

  @Override
  public boolean isDirect() {
    return byteBuf.isDirect();
  }

  @Override
  public int readerIndex() {
    return byteBuf.readerIndex();
  }

  @Override
  public ByteBuf readerIndex(int readerIndex) {
    return byteBuf.readerIndex(readerIndex);
  }

  @Override
  public int writerIndex() {
    return byteBuf.writerIndex();
  }

  @Override
  public ByteBuf writerIndex(int writerIndex) {
    return byteBuf.writerIndex(writerIndex);
  }

  @Override
  public ByteBuf setIndex(int readerIndex, int writerIndex) {
    return byteBuf.setIndex(readerIndex, writerIndex);
  }

  @Override
  public int readableBytes() {
    return byteBuf.readableBytes();
  }

  @Override
  public int writableBytes() {
    return byteBuf.writableBytes();
  }

  @Override
  public int maxWritableBytes() {
    return byteBuf.maxWritableBytes();
  }

  @Override
  public boolean isReadable() {
    return byteBuf.isReadable();
  }

  @Override
  public boolean isReadable(int size) {
    return byteBuf.isReadable(size);
  }

  @Override
  public boolean isWritable() {
    return byteBuf.isWritable();
  }

  @Override
  public boolean isWritable(int size) {
    return byteBuf.isWritable(size);
  }

  @Override
  public ByteBuf clear() {
    return byteBuf.clear();
  }

  @Override
  public ByteBuf markReaderIndex() {
    return byteBuf.markReaderIndex();
  }

  @Override
  public ByteBuf resetReaderIndex() {
    return byteBuf.resetReaderIndex();
  }

  @Override
  public ByteBuf markWriterIndex() {
    return byteBuf.markWriterIndex();
  }

  @Override
  public ByteBuf resetWriterIndex() {
    return byteBuf.resetWriterIndex();
  }

  @Override
  public ByteBuf discardReadBytes() {
    return byteBuf.discardReadBytes();
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    return byteBuf.discardSomeReadBytes();
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    return byteBuf.ensureWritable(minWritableBytes);
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    return byteBuf.ensureWritable(minWritableBytes, force);
  }

  @Override
  public boolean getBoolean(int index) {
    return byteBuf.getBoolean(index);
  }

  @Override
  public byte getByte(int index) {
    return byteBuf.getByte(index);
  }

  @Override
  public short getUnsignedByte(int index) {
    return byteBuf.getUnsignedByte(index);
  }

  @Override
  public short getShort(int index) {
    return byteBuf.getShort(index);
  }

  @Override
  public int getUnsignedShort(int index) {
    return byteBuf.getUnsignedShort(index);
  }

  @Override
  public int getMedium(int index) {
    return byteBuf.getMedium(index);
  }

  @Override
  public int getUnsignedMedium(int index) {
    return byteBuf.getUnsignedMedium(index);
  }

  @Override
  public int getInt(int index) {
    return byteBuf.getInt(index);
  }

  @Override
  public long getUnsignedInt(int index) {
    return byteBuf.getUnsignedInt(index);
  }

  @Override
  public long getLong(int index) {
    return byteBuf.getLong(index);
  }

  @Override
  public char getChar(int index) {
    return byteBuf.getChar(index);
  }

  @Override
  public float getFloat(int index) {
    return byteBuf.getFloat(index);
  }

  @Override
  public double getDouble(int index) {
    return byteBuf.getDouble(index);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst) {
    return byteBuf.getBytes(index, dst);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int length) {
    return byteBuf.getBytes(index, dst, length);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    return byteBuf.getBytes(index, dst, dstIndex, length);
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst) {
    return byteBuf.getBytes(index, dst);
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    return byteBuf.getBytes(index, dst, dstIndex, length);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    return byteBuf.getBytes(index, dst);
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    return byteBuf.getBytes(index, out, length);
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return byteBuf.getBytes(index, out, length);
  }

  @Override
  public ByteBuf setBoolean(int index, boolean value) {
    return byteBuf.setBoolean(index, value);
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    return byteBuf.setByte(index, value);
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    return byteBuf.setShort(index, value);
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    return byteBuf.setMedium(index, value);
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    return byteBuf.setInt(index, value);
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    return byteBuf.setLong(index, value);
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    return byteBuf.setChar(index, value);
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    return byteBuf.setFloat(index, value);
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    return byteBuf.setDouble(index, value);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src) {
    return byteBuf.setBytes(index, src);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int length) {
    return byteBuf.setBytes(index, src, length);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    return byteBuf.setBytes(index, src, srcIndex, length);
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src) {
    return byteBuf.setBytes(index, src);
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    return byteBuf.setBytes(index, src, srcIndex, length);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    return byteBuf.setBytes(index, src);
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return byteBuf.setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return byteBuf.setBytes(index, in, length);
  }

  @Override
  public ByteBuf setZero(int index, int length) {
    return byteBuf.setZero(index, length);
  }

  @Override
  public boolean readBoolean() {
    return byteBuf.readBoolean();
  }

  @Override
  public byte readByte() {
    return byteBuf.readByte();
  }

  @Override
  public short readUnsignedByte() {
    return byteBuf.readUnsignedByte();
  }

  @Override
  public short readShort() {
    return byteBuf.readShort();
  }

  @Override
  public int readUnsignedShort() {
    return byteBuf.readUnsignedShort();
  }

  @Override
  public int readMedium() {
    return byteBuf.readMedium();
  }

  @Override
  public int readUnsignedMedium() {
    return byteBuf.readUnsignedMedium();
  }

  @Override
  public int readInt() {
    return byteBuf.readInt();
  }

  @Override
  public long readUnsignedInt() {
    return byteBuf.readUnsignedInt();
  }

  @Override
  public long readLong() {
    return byteBuf.readLong();
  }

  @Override
  public char readChar() {
    return byteBuf.readChar();
  }

  @Override
  public float readFloat() {
    return byteBuf.readFloat();
  }

  @Override
  public double readDouble() {
    return byteBuf.readDouble();
  }

  @Override
  public ByteBuf readBytes(int length) {
    return byteBuf.readBytes(length);
  }

  @Override
  public ByteBuf readSlice(int length) {
    return byteBuf.readSlice(length);
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst) {
    return byteBuf.readBytes(dst);
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst, int length) {
    return byteBuf.readBytes(dst, length);
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
    return byteBuf.readBytes(dst, dstIndex, length);
  }

  @Override
  public ByteBuf readBytes(byte[] dst) {
    return byteBuf.readBytes(dst);
  }

  @Override
  public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
    return byteBuf.readBytes(dst, dstIndex, length);
  }

  @Override
  public ByteBuf readBytes(ByteBuffer dst) {
    return byteBuf.readBytes(dst);
  }

  @Override
  public ByteBuf readBytes(OutputStream out, int length) throws IOException {
    return byteBuf.readBytes(out, length);
  }

  @Override
  public int readBytes(GatheringByteChannel out, int length) throws IOException {
    return byteBuf.readBytes(out, length);
  }

  @Override
  public ByteBuf skipBytes(int length) {
    return byteBuf.skipBytes(length);
  }

  @Override
  public ByteBuf writeBoolean(boolean value) {
    return byteBuf.writeBoolean(value);
  }

  @Override
  public ByteBuf writeByte(int value) {
    return byteBuf.writeByte(value);
  }

  @Override
  public ByteBuf writeShort(int value) {
    return byteBuf.writeShort(value);
  }

  @Override
  public ByteBuf writeMedium(int value) {
    return byteBuf.writeMedium(value);
  }

  @Override
  public ByteBuf writeInt(int value) {
    return byteBuf.writeInt(value);
  }

  @Override
  public ByteBuf writeLong(long value) {
    return byteBuf.writeLong(value);
  }

  @Override
  public ByteBuf writeChar(int value) {
    return byteBuf.writeChar(value);
  }

  @Override
  public ByteBuf writeFloat(float value) {
    return byteBuf.writeFloat(value);
  }

  @Override
  public ByteBuf writeDouble(double value) {
    return byteBuf.writeDouble(value);
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src) {
    return byteBuf.writeBytes(src);
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int length) {
    return byteBuf.writeBytes(src, length);
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
    return byteBuf.writeBytes(src, srcIndex, length);
  }

  @Override
  public ByteBuf writeBytes(byte[] src) {
    return byteBuf.writeBytes(src);
  }

  @Override
  public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
    return byteBuf.writeBytes(src, srcIndex, length);
  }

  @Override
  public ByteBuf writeBytes(ByteBuffer src) {
    return byteBuf.writeBytes(src);
  }

  @Override
  public int writeBytes(InputStream in, int length) throws IOException {
    return byteBuf.writeBytes(in, length);
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
    return byteBuf.writeBytes(in, length);
  }

  @Override
  public ByteBuf writeZero(int length) {
    return byteBuf.writeZero(length);
  }

  @Override
  public int indexOf(int fromIndex, int toIndex, byte value) {
    return byteBuf.indexOf(fromIndex, toIndex, value);
  }

  @Override
  public int bytesBefore(byte value) {
    return byteBuf.bytesBefore(value);
  }

  @Override
  public int bytesBefore(int length, byte value) {
    return byteBuf.bytesBefore(length, value);
  }

  @Override
  public int bytesBefore(int index, int length, byte value) {
    return byteBuf.bytesBefore(index, length, value);
  }

  @Override
  public int forEachByte(ByteBufProcessor processor) {
    return byteBuf.forEachByte(processor);
  }

  @Override
  public int forEachByte(int index, int length, ByteBufProcessor processor) {
    return byteBuf.forEachByte(index, length, processor);
  }

  @Override
  public int forEachByteDesc(ByteBufProcessor processor) {
    return byteBuf.forEachByteDesc(processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
    return byteBuf.forEachByteDesc(index, length, processor);
  }

  @Override
  public ByteBuf copy() {
    return byteBuf.copy();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return byteBuf.copy(index, length);
  }

  @Override
  public ByteBuf slice() {
    return byteBuf.slice();
  }

  @Override
  public ByteBuf slice(int index, int length) {
    return byteBuf.slice(index, length);
  }

  @Override
  public ByteBuf duplicate() {
    return byteBuf.duplicate();
  }

  @Override
  public int nioBufferCount() {
    return byteBuf.nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer() {
    return byteBuf.nioBuffer();
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return byteBuf.nioBuffer(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return byteBuf.internalNioBuffer(index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    return byteBuf.nioBuffers();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return byteBuf.nioBuffers(index, length);
  }

  @Override
  public boolean hasArray() {
    return byteBuf.hasArray();
  }

  @Override
  public byte[] array() {
    return byteBuf.array();
  }

  @Override
  public int arrayOffset() {
    return byteBuf.arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return byteBuf.hasMemoryAddress();
  }

  @Override
  public long memoryAddress() {
    return byteBuf.memoryAddress();
  }

  @Override
  public String toString(Charset charset) {
    return byteBuf.toString(charset);
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    return byteBuf.toString(index, length, charset);
  }

  @Override
  public int hashCode() {
    return byteBuf.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return byteBuf.equals(obj);
  }

  @Override
  public int compareTo(ByteBuf buffer) {
    return byteBuf.compareTo(buffer);
  }

  @Override
  public String toString() {
    return byteBuf.toString();
  }

  @Override
  public ByteBuf retain(int increment) {
    refCount += increment;
    return byteBuf.retain(increment);
  }

  @Override
  public ByteBuf retain() {
    refCount++;
    return byteBuf.retain();
  }

  @Override
  public int refCnt() {
    return byteBuf.refCnt();
  }

  @Override
  public boolean release() {
    synchronized (this) {
      refCount--;
      if (refCount == 0) {
        allocator.free(byteBuf);
      } else if (refCount < 0) {
        logger.error("Ref count zero .");
        throw new DrillRuntimeException("Ref count zero");
      }
      return byteBuf.release();
    }
  }

  @Override
  public boolean release(int decrement) {
    refCount -= decrement;
    return byteBuf.release(decrement);
  }
}
