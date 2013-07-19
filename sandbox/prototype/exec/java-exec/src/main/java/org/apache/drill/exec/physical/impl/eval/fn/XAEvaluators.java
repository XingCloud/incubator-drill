package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordPointer;
import org.apache.drill.exec.record.vector.Fixed8;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.VarLen4;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes.*;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 7/16/13
 * Time: 12:02 PM
 */
public class XAEvaluators {


    public  static abstract class  XAEvaluator extends BaseBasicEvaluator {
        protected BasicEvaluator child;
        private VarLen4 timeStr;

        public XAEvaluator(RecordPointer record, FunctionArguments args) {
            super(args.isOnlyConstants(), record);
            child = args.getOnlyEvaluator();
        }

        @Override
        public VarLen4 eval() {
            int period = getPeriod();
            Fixed8 v = (Fixed8) child.eval();
            if (timeStr == null) {
                timeStr = new VarLen4(MaterializedField.create(new SchemaPath(getFieldName()), 1, 0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.VARBINARY4)), BufferAllocator.getAllocator(null));
                timeStr.allocateNew(v.getRecordCount());
            }
            if (timeStr.getRecordCount() < v.getRecordCount()) {
                timeStr.allocateNew(v.getRecordCount());
            }
            timeStr.setRecordCount(v.getRecordCount());
            for (int i = 0; i < timeStr.getRecordCount(); i++) {
                timeStr.setBytes(i, getKeyBySpecificPeriod(v.getBigInt(i), period).getBytes());
            }
            return timeStr;

        }

        public abstract String getFieldName();

        public abstract int getPeriod();
    }


    @FunctionEvaluator("min5")
    public static class Min5Evaluator extends XAEvaluator {

        public Min5Evaluator(RecordPointer record, FunctionArguments args) {
            super(record, args);
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
        public HourEvaluator(RecordPointer record, FunctionArguments args) {
            super(record, args);
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
