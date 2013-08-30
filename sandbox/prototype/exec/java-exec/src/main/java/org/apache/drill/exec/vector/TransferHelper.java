package org.apache.drill.exec.vector;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/31/13
 * Time: 6:00 PM
 */
public class TransferHelper {

  public static List<VectorWrapper<?>> transferVectors(Iterable<VectorWrapper<?>> vectors) {
    List<VectorWrapper<?>> newVectors = Lists.newArrayList();
    for (VectorWrapper<?> v : vectors) {
      newVectors.add(v.cloneAndTransfer());
    }
    return newVectors;
  }

  public static ValueVector transferVector(ValueVector v) {
    TransferPair transferPair = v.getTransferPair();
    transferPair.transfer();
    return transferPair.getTo();
  }

  public static ValueVector mirrorVector(ValueVector v) {
    TransferPair tp = v.getTransferPair();
    tp.mirror();
    return tp.getTo();
  }


}
