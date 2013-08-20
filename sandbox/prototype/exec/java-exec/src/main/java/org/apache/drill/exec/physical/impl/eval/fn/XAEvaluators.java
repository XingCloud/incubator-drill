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
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

import java.text.SimpleDateFormat;
import java.util.Date;
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
    }

    @Override
    public VarCharVector eval() {
      int period = getPeriod();

      if (timeStr == null) {
        timeStr = new VarCharVector(MaterializedField.create(new SchemaPath("XA", ExpressionPosition.UNKNOWN),
          Types.required(TypeProtos.MinorType.VARCHAR))
          , recordBatch.getContext().getAllocator());
      }

      BigIntVector bigIntVector =   (BigIntVector) child.eval() ;
      BigIntVector.Accessor accessor = bigIntVector.getAccessor();
      int recordCount = accessor.getValueCount();
      timeStr.allocateNew(40 * recordCount, recordCount);
      VarCharVector.Mutator mutator = timeStr.getMutator();


      for (int i = 0; i < recordCount; i++) {
        mutator.set(i, getKeyBySpecificPeriod(accessor.get(i), period).getBytes());
      }

      bigIntVector.close();
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

    @FunctionEvaluator("date")
    public static class DateEvaluator extends BaseBasicEvaluator {

      RecordBatch recordBatch;
      BasicEvaluator child;
      private VarCharVector varCharVector;

      public DateEvaluator(RecordBatch recordBatch, FunctionArguments args) {
        super(args.isOnlyConstants(), recordBatch);
        this.recordBatch = recordBatch;
        child = args.getOnlyEvaluator();
      }

      @Override
      public VarCharVector eval() {
        if (varCharVector == null) {
          varCharVector = new VarCharVector(MaterializedField.create(new SchemaPath("date", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARCHAR)),
            recordBatch.getContext().getAllocator());
        }
        BigIntVector bigIntVector = (BigIntVector) child.eval();
        BigIntVector.Accessor accessor = bigIntVector.getAccessor();
        int recordCount = accessor.getValueCount();
        varCharVector.allocateNew(20 * recordCount, recordCount);
        VarCharVector.Mutator mutator = varCharVector.getMutator();
        for (int i = 0; i < recordCount; i++) {
          mutator.set(i, getDateString(accessor.get(i)).getBytes());
        }
        mutator.setValueCount(recordCount);
        bigIntVector.close();
        return varCharVector;
      }

      private String getDateString(long l) {
        String sqlDate = String.valueOf(l) ;
        return sqlDate.substring(0,4) + "-" + sqlDate.substring(4,6) + "-" + sqlDate.substring(6,8);
      }
    }


    @FunctionEvaluator("hid2inner")
    public static class Hid2Inner extends BaseBasicEvaluator {
      RecordBatch recordBatch;
      BasicEvaluator child;
      private IntVector intVector;

      public Hid2Inner(RecordBatch recordBatch, FunctionArguments args) {
        super(args.isOnlyConstants(), recordBatch);
        this.recordBatch = recordBatch;
        child = args.getOnlyEvaluator();
      }

      @Override
      public IntVector eval() {
        if (intVector == null) {
          intVector = new IntVector(MaterializedField.create(new SchemaPath("uid", ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.INT)),
            recordBatch.getContext().getAllocator());
        }

        BigIntVector bigIntVector = (BigIntVector) child.eval();
        BigIntVector.Accessor accessor = bigIntVector.getAccessor();
        int recordCount = accessor.getValueCount();
        IntVector.Mutator mutator = intVector.getMutator();
        intVector.allocateNew(recordCount);
        for (int i = 0; i < recordCount; i++) {
          mutator.set(i, getInnerUidFromSamplingUid(accessor.get(i)));
        }
        bigIntVector.close();
        mutator.setValueCount(recordCount);
        return intVector;
      }

      private int getInnerUidFromSamplingUid(long suid) {
        return (int) (0xffffffffl & suid);
      }
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
