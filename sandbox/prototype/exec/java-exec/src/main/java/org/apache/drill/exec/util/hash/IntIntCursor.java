package org.apache.drill.exec.util.hash;

/**
 * User: liuxiong
 * Date: 13-9-9
 * Time: 下午5:18
 */
public final class IntIntCursor {

  /**
   * The current key and value's index in the container this cursor belongs to. The meaning of
   * this index is defined by the container (usually it will be an index in the underlying
   * storage buffer).
   */
  public int index;

  /**
   * The current key.
   */
  public int key;

  /**
   * The current value.
   */
  public int value;

  @Override
  public String toString(){
    return "[cursor, index: " + index + ", key: " + key + ", value: " + value + "]";
  }
}
