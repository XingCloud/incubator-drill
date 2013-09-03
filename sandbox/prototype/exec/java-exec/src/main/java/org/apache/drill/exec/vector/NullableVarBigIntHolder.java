package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.ValueHolder;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-2
 * Time: 下午5:25
 * To change this template use File | Settings | File Templates.
 */
public class NullableVarBigIntHolder implements ValueHolder {
  public static final TypeProtos.MajorType TYPE = Types.optional(TypeProtos.MinorType.VARBIGINT);

  public static final int WIDTH = 8;
  /** Whether the given holder holds a valid value.  1 means non-null.  0 means null. **/
  public int isSet;

  /** The first offset (inclusive) into the buffer. **/
  public int start;

  /** The last offset (exclusive) into the buffer. **/
  public int end;

  /** The buffer holding actual values. **/
  public ByteBuf buffer;

}
