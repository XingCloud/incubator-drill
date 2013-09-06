package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.ValueHolder;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-2
 * Time: 下午2:45
 * To change this template use File | Settings | File Templates.
 */
public class VarBigIntHolder implements ValueHolder {
  public static final TypeProtos.MajorType TYPE = Types.required(TypeProtos.MinorType.VARBIGINT);

  public static final int WIDTH = 8;

  /** The first offset (inclusive) into the buffer. **/
  public int start;

  /** The last offset (exclusive) into the buffer. **/
  public int end;

  /** The buffer holding actual values. **/
  public ByteBuf buffer;

}
