package org.apache.drill.exec.physical.impl.eval.fn;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.eval.BaseBasicEvaluator;
import org.apache.drill.exec.physical.impl.eval.EvaluatorTypes;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.DrillValue;
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



    @FunctionEvaluator("min5")
    public static class Min5Evaluator extends BaseBasicEvaluator {
        private final static int MIN5 = 5;
        private BasicEvaluator evaluator;

        public Min5Evaluator(RecordPointer record, FunctionArguments args) {
            super(args.isOnlyConstants(), record);
            evaluator = args.getOnlyEvaluator();
        }

        @Override
        public VarLen4 eval() {
            VarLen4 varLen4 = new VarLen4(MaterializedField.create(new SchemaPath("min5"),1,0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.VARBINARY4)), BufferAllocator.getAllocator(null));
            return varLen4;
        }
    }


    @FunctionEvaluator("hour")
    public static class HourEvaluator extends BaseBasicEvaluator {
        private final static int HOUR = 60;

        private BasicEvaluator evaluator;

        public HourEvaluator(RecordPointer record, FunctionArguments args) {
            super(args.isOnlyConstants(), record);
            evaluator = args.getOnlyEvaluator();
        }

        @Override
        public VarLen4 eval() {
            VarLen4 varLen4 = new VarLen4(MaterializedField.create(new SchemaPath("hour"),1,0, TypeHelper.getMajorType(SchemaDefProtos.DataMode.REQUIRED, SchemaDefProtos.MinorType.VARBINARY4)), BufferAllocator.getAllocator(null));
            return varLen4;
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
