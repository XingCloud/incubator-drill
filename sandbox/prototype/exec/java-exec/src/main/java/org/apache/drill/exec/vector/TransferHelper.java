package org.apache.drill.exec.vector;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/31/13
 * Time: 6:00 PM
 */
public class TransferHelper {

  public static List<ValueVector> transferVectors(Iterable<ValueVector> vectors) {
    List<ValueVector> newVectors = Lists.newArrayList();
    for (ValueVector v : vectors) {
      newVectors.add(transferVector(v));
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
