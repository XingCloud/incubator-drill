package org.apache.drill.exec.util.hash;

/**
 * User: liuxiong
 * Date: 13-9-9
 * Time: 下午3:54
 */
public class HashMapUtils {

  /**
   * Maximum capacity for an array that is of power-of-two size and still
   * allocatable in Java (not a negative int).
   */
  final static int MAX_CAPACITY = 0x80000000 >>> 1;

  /**
   * Minimum capacity for a hash container.
   */
  final static int MIN_CAPACITY = 4;

  /**
   * Default capacity for a hash container.
   */
  final static int DEFAULT_CAPACITY = 16;

  /**
   * Default load factor.
   */
  final static float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * Round the capacity to the next allowed value.
   */
  static int roundCapacity(int requestedCapacity)  {
    if (requestedCapacity > MAX_CAPACITY)
      return MAX_CAPACITY;

    return Math.max(MIN_CAPACITY, nextHighestPowerOfTwo(requestedCapacity));
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
  static int nextHighestPowerOfTwo(int v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }

  /**
   * Return the next possible capacity, counting from the current buffers'
   * size.
   */
  static int nextCapacity(int current){
    assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two.";

    if (current < MIN_CAPACITY / 2){
      current = MIN_CAPACITY / 2;
    }

    current <<= 1;
    if (current < 0){
      throw new RuntimeException("Maximum capacity exceeded.");
    }

    return current;
  }

  /**
   * Hashes a 4-byte sequence (Java int).
   */
  static int hash(int k){
    k ^= k >>> 16;
    k *= 0x85ebca6b;
    k ^= k >>> 13;
    k *= 0xc2b2ae35;
    k ^= k >>> 16;
    return k;
  }
}
