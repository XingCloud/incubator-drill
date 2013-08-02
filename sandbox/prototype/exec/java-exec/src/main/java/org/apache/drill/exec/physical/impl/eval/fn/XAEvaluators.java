package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 12:02 PM
 */
public class XAEvaluators {


  public static abstract class XAEvaluator extends BaseBasicEvaluator {
    protected BasicEvaluator child;
    private VarCharVector timeStr;

    public XAEvaluator(RecordBatch recordBatch, FunctionArguments args) {
      super(args.isOnlyConstants(), recordBatch);
      child = args.getOnlyEvaluator();
      timeStr = new VarCharVector(MaterializedField.create(new SchemaPath("XA", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.VARCHAR))
        , recordBatch.getContext().getAllocator());
    }

    @Override
    public VarCharVector eval() {
      int period = getPeriod();

      BigIntVector.Accessor accessor = ((BigIntVector) child.eval()).getAccessor();
      int recordCount = accessor.getValueCount();
      timeStr.allocateNew(20 * recordCount, recordCount);
      VarCharVector.Mutator mutator = timeStr.getMutator();


      for (int i = 0; i < recordCount; i++) {
        mutator.set(i, getKeyBySpecificPeriod(accessor.get(i), period).getBytes());
      }
      mutator.setValueCount(recordCount);
      return timeStr;

    }

    public abstract String getFieldName();

    public abstract int getPeriod();
  }


  @FunctionEvaluator("min5")
  public static class Min5Evaluator extends XAEvaluator {

    public Min5Evaluator(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public String getFieldName() {
      return "min5";
    }

    @Override
    public int getPeriod() {
      return 5;
    }
  }


  @FunctionEvaluator("hour")
  public static class HourEvaluator extends XAEvaluator {
    public HourEvaluator(RecordBatch recordBatch, FunctionArguments args) {
      super(recordBatch, args);
    }

    @Override
    public String getFieldName() {
      return "hour";
    }

    @Override
    public int getPeriod() {
      return 60;
    }
  }


  public static String getKeyBySpecificPeriod(long timestamp, int period) {
    String ID = "GMT+8";
    TimeZone tz = TimeZone.getTimeZone(ID);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    sf.setTimeZone(tz);

    String[] yhm = sf.format(timestamp).split(" ");
    String[] hm = yhm[1].split(":");
    int minutes = Integer.parseInt(hm[1]);

    int val = minutes % period;
    if (val != 0) {
      minutes = minutes - val;
    }

    String minutesStr = String.valueOf(minutes);
    if (minutes < 10) {
      minutesStr = "0" + minutesStr;
    }

    return yhm[0] + " " + hm[0] + ":" + minutesStr;
  }
}
