/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import io.netty.util.Pair;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

final class PoolChunk<T> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PoolChunk.class);

    private static final int SWAP_UPPER_LIMIT = 4 << 20;    //4MB

    // chunk status
    private static final int ST_UNUSED = 0;
    private static final int ST_BRANCH = 1;
    private static final int ST_ALLOCATED = 2;
    private static final int ST_ALLOCATED_SUBPAGE = ST_ALLOCATED | 1; // 3

    // todo: what's the meaning of these magic numbers?
    // used to generate random number
    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;
    private static final long mask = (1L << 48) - 1;

    private static final AtomicLong nextChunkId = new AtomicLong(1);
    private final long id;

    final PoolArena<T> arena;
    final T memory;
    final boolean unpooled; // used for huge allocation

    private final int[] memoryMap;  // representing binary heap; used in binary buddy algorithm
    private final PoolSubpage<T>[] subpages;
    /** Used to determine if the requested capacity is equal to or greater than pageSize. */
    private final int subpageOverflowMask;
    private final int pageSize;
    private final int pageShifts;

    private final int chunkSize;
    private final int maxSubpageAllocs;

    private long random = (System.nanoTime() ^ multiplier) & mask;

    private int freeBytes;

    PoolChunkList<T> parent;
    PoolChunk<T> prev;
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolChunk(PoolArena<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize) {
        id = nextChunkId.getAndIncrement();
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        subpageOverflowMask = ~(pageSize - 1);
        freeBytes = chunkSize;

        int chunkSizeInPages = chunkSize >>> pageShifts;
        maxSubpageAllocs = 1 << maxOrder;

        // Generate the memory map.
        memoryMap = new int[maxSubpageAllocs << 1];
        int memoryMapIndex = 1;
        for (int i = 0; i <= maxOrder; i ++) {
            int runSizeInPages = chunkSizeInPages >>> i;
            for (int j = 0; j < chunkSizeInPages; j += runSizeInPages) {
                //noinspection PointlessBitwiseExpression
                memoryMap[memoryMapIndex ++] = j << 17 | runSizeInPages << 2 | ST_UNUSED;
            }
        }

        subpages = newSubpageArray(maxSubpageAllocs);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, T memory, int size) {
        id = nextChunkId.getAndIncrement();
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        memoryMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        chunkSize = size;
        maxSubpageAllocs = 0;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpageArray(int size) {
        return new PoolSubpage[size];
    }

    int usage() {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    Pair<PoolChunk<T>, Long> findSwappable(PooledByteBuf<T> buf, int normCapacity) {
        ConcurrentHashMapV8<Pair<Long, Long>, Long> inMemoryMap = PooledByteBuf.getInMemoryMap();
        ConcurrentHashMapV8<Long, int[]> onDiskMap = PooledByteBuf.getOnDiskMap();

        final int upperLimit = Math.min(chunkSize, SWAP_UPPER_LIMIT);
        // do not swap sub-page
        final int lowerLimit = Math.max(normCapacity, pageSize << 1);
        if (lowerLimit > upperLimit) {
            return null;
        }

        for (int curIdx = 1; curIdx < memoryMap.length; ++curIdx) {
            int val = memoryMap[curIdx];
            int length = runLength(val);

            // length should be in range [lowerLimit, upperLimit]
            if (length > upperLimit) {
                continue;
            } else if (length < lowerLimit) {
                break;
            }

            if ((val & 3) == ST_ALLOCATED) {
                Pair<Long, Long> inMemoryKey = new Pair<Long, Long>(id, (long)curIdx);

                assert inMemoryMap.containsKey(inMemoryKey);
                assert !onDiskMap.containsKey(inMemoryMap.get(inMemoryKey));
                assert inMemoryMap.get(inMemoryKey) != buf.id;

                logger.debug("found: (" + id + ", " + curIdx + ")");

                return new Pair<PoolChunk<T>, Long>(this, (long)curIdx);
            }
        }

//        System.out.println("chunk: " + id + ", not found");

        return null;
    }

    long allocate(int normCapacity) {
        int firstVal = memoryMap[1];
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize
            return allocateRun(normCapacity, 1, firstVal);
        } else {
            return allocateSubpage(normCapacity, 1, firstVal);
        }
    }

    private long allocateRun(int normCapacity, int curIdx, int val) {
        for (;;) {
            if ((val & ST_ALLOCATED) != 0) { // state == ST_ALLOCATED || state == ST_ALLOCATED_SUBPAGE
                return -1;
            }

            if ((val & ST_BRANCH) != 0) { // state == ST_BRANCH
                // randomly get one of the two children
                int nextIdx = curIdx << 1 ^ nextRandom();
                long res = allocateRun(normCapacity, nextIdx, memoryMap[nextIdx]);
                if (res > 0) {
                    return res;
                }

                curIdx = nextIdx ^ 1;
                val = memoryMap[curIdx];
                continue;
            }

            // state == ST_UNUSED
            return allocateRunSimple(normCapacity, curIdx, val);
        }
    }

    private long allocateRunSimple(int normCapacity, int curIdx, int val) {
        int runLength = runLength(val);
        if (normCapacity > runLength) {
            return -1;
        }

        for (;;) {
            if (normCapacity == runLength) {
                // Found the run that fits.
                // Note that capacity has been normalized already, so we don't need to deal with
                // the values that are not power of 2.
                memoryMap[curIdx] = val & ~3 | ST_ALLOCATED;
                freeBytes -= runLength;
                return curIdx;
            }

            int nextIdx = curIdx << 1 ^ nextRandom();
            int unusedIdx = nextIdx ^ 1;

            memoryMap[curIdx] = val & ~3 | ST_BRANCH;
            //noinspection PointlessBitwiseExpression
            memoryMap[unusedIdx] = memoryMap[unusedIdx] & ~3 | ST_UNUSED;

            runLength >>>= 1;
            curIdx = nextIdx;
            val = memoryMap[curIdx];
        }
    }

    private long allocateSubpage(int normCapacity, int curIdx, int val) {
        int state = val & 3;
        if (state == ST_BRANCH) {
            int nextIdx = curIdx << 1 ^ nextRandom();
            long res = branchSubpage(normCapacity, nextIdx);
            if (res > 0) {
                return res;
            }

            return branchSubpage(normCapacity, nextIdx ^ 1);
        }

        if (state == ST_UNUSED) {
            return allocateSubpageSimple(normCapacity, curIdx, val);
        }

        if (state == ST_ALLOCATED_SUBPAGE) {
            PoolSubpage<T> subpage = subpages[subpageIdx(curIdx)];
            int elemSize = subpage.elemSize;
            if (normCapacity != elemSize) {
                return -1;
            }

            return subpage.allocate();
        }

        return -1;
    }

    private long allocateSubpageSimple(int normCapacity, int curIdx, int val) {
        int runLength = runLength(val);
        for (;;) {
            if (runLength == pageSize) {
                memoryMap[curIdx] = val & ~3 | ST_ALLOCATED_SUBPAGE;
                freeBytes -= runLength;

                int subpageIdx = subpageIdx(curIdx);
                PoolSubpage<T> subpage = subpages[subpageIdx];
                if (subpage == null) {
                    subpage = new PoolSubpage<T>(this, curIdx, runOffset(val), pageSize, normCapacity);
                    subpages[subpageIdx] = subpage;
                } else {
                    subpage.init(normCapacity);
                }
                return subpage.allocate();
            }

            int nextIdx = curIdx << 1 ^ nextRandom();
            int unusedIdx = nextIdx ^ 1;

            memoryMap[curIdx] = val & ~3 | ST_BRANCH;
            //noinspection PointlessBitwiseExpression
            memoryMap[unusedIdx] = memoryMap[unusedIdx] & ~3 | ST_UNUSED;

            runLength >>>= 1;
            curIdx = nextIdx;
            val = memoryMap[curIdx];
        }
    }

    private long branchSubpage(int normCapacity, int nextIdx) {
        int nextVal = memoryMap[nextIdx];
        if ((nextVal & 3) != ST_ALLOCATED) {
            return allocateSubpage(normCapacity, nextIdx, nextVal);
        }
        return -1;
    }

    void free(long handle) {
        int memoryMapIdx = (int) handle;
        int bitmapIdx = (int) (handle >>> 32);

        int val = memoryMap[memoryMapIdx];
        int state = val & 3;
        if (state == ST_ALLOCATED_SUBPAGE) {
            assert bitmapIdx != 0;
            PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;
            if (subpage.free(bitmapIdx & 0x3FFFFFFF)) {
                return;
            }
        } else {
            assert state == ST_ALLOCATED : "state: " + state;
            assert bitmapIdx == 0;
        }

        freeBytes += runLength(val);

        for (;;) {
            //noinspection PointlessBitwiseExpression
            memoryMap[memoryMapIdx] = val & ~3 | ST_UNUSED;
            if (memoryMapIdx == 1) {
                assert freeBytes == chunkSize;
                return;
            }

            if ((memoryMap[siblingIdx(memoryMapIdx)] & 3) != ST_UNUSED) {
                break;
            }

            memoryMapIdx = parentIdx(memoryMapIdx);
            val = memoryMap[memoryMapIdx];
        }
    }

    void initBuf(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        int memoryMapIdx = (int) handle;
        int bitmapIdx = (int) (handle >>> 32);
        if (bitmapIdx == 0) {
            int val = memoryMap[memoryMapIdx];
            assert (val & 3) == ST_ALLOCATED : String.valueOf(val & 3);
            buf.init(this, handle, runOffset(val), reqCapacity, runLength(val));
        } else {
            initBufWithSubpage(buf, handle, bitmapIdx, reqCapacity);
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        initBufWithSubpage(buf, handle, (int) (handle >>> 32), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        int memoryMapIdx = (int) handle;
        int val = memoryMap[memoryMapIdx];
        assert (val & 3) == ST_ALLOCATED_SUBPAGE;

        PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        buf.init(
                this, handle,
                runOffset(val) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize, reqCapacity, subpage.elemSize);
    }

    private static int parentIdx(int memoryMapIdx) {
        return memoryMapIdx >>> 1;
    }

    private static int siblingIdx(int memoryMapIdx) {
        return memoryMapIdx ^ 1;
    }

    int runLength(int val) {
        return (val >>> 2 & 0x7FFF) << pageShifts;
    }

    int runOffset(int val) {
        return val >>> 17 << pageShifts;
    }

    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx - maxSubpageAllocs;
    }

    private int nextRandom() {
        random = random * multiplier + addend & mask;
        return (int) (random >>> 47) & 1;
    }

    /**
     * for test only
     */
    public static long getNextChunkId() {
        return nextChunkId.get();
    }

    long getId() {
        return id;
    }

    int[] getMemoryMap() {
        return memoryMap;
    }

    @Override
    public int hashCode() {
        return Long.valueOf(id).hashCode();
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Chunk(");
        buf.append(id);
        buf.append(": ");
        buf.append(usage());
        buf.append("%, ");
        buf.append(chunkSize - freeBytes);
        buf.append('/');
        buf.append(chunkSize);
        buf.append(')');
        return buf.toString();
    }

}
