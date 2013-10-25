package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 10/12/13
 * Time: 5:40 PM
 */
public class SingleAllocator extends BufferAllocator {

  BufferAllocator buffer;
  AtomicInteger allocatedSize = new AtomicInteger(0);

  public SingleAllocator(BufferAllocator buffer) {
    this.buffer = buffer;
  }

  @Override
  public ByteBuf buffer(int size) {
    ByteBuf byteBuf = buffer.buffer(size);
    allocatedSize.addAndGet(byteBuf.capacity());
    return new WrappedByteBuf(byteBuf, this);
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return buffer.getUnderlyingAllocator();
  }

  @Override
  public BufferAllocator getChildAllocator(long initialReservation, long maximumReservation) {
    return buffer.getChildAllocator(initialReservation, maximumReservation);
  }

  @Override
  protected boolean pre(int bytes) {
    return buffer.pre(bytes);
  }

  @Override
  public void close() {
    logger.info("Allocated direct memory : {} bytes .", buffer.getAllocatedMemory());
    int size = allocatedSize.get();
    if (size != 0) {
      logger.error("Memory leak exist , {} bytes are not released .", size);
    }
  }

  @Override
  public long getAllocatedMemory() {
    return 0;
  }

  @Override
  public long free(ByteBuf byteBuf) {
    int size = byteBuf.capacity();
    allocatedSize.addAndGet(-size);
    buffer.free(byteBuf);
    return size;
  }
}
