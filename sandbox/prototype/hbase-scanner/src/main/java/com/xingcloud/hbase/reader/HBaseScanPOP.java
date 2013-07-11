package com.xingcloud.hbase.reader;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 7/3/13
 * Time: 3:22 AM
 * To change this template use File | Settings | File Templates.
 */
/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.SchemaDefProtos;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//import org.apache.drill.exec.ref.rops.ROP;


@JsonTypeName("hbase-scan")
public class HBaseScanPOP extends AbstractScan<HBaseScanPOP.HBaseScanEntry> {
    //static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseScanPOP.class);

    private final String url;
    private  LinkedList<HBaseScanEntry>[] mappings;
    //private ROP parent;

    @JsonCreator
    public HBaseScanPOP(@JsonProperty("url") String url, @JsonProperty("entries") List<HBaseScanEntry> readEntries) {
        super(readEntries);
        this.url = url;
        //this.parent=parent;
    }

    public String getUrl() {
        return url;
    }
    /*
    public ROP getParent(){
        return parent;
    }
    */

    public static class HBaseScanEntry implements ReadEntry {
        private final ScanType[] types;
        //private final int recordSize;
        private final String eventPattern;
        private String pID;

        public List<String> getDayList() {
            return dayList;
        }

        private List<String> dayList;
        @JsonCreator
        public HBaseScanEntry(@JsonProperty("types") ScanType[] types, @JsonProperty("days")List<String> dayList,
                              @JsonProperty("event") String eventPattern,@JsonProperty("project_id")String pID){
            this.types = types;
            //int size = 0;
            //this.recordSize = size;
            this.eventPattern=eventPattern;
            this.pID=pID;
            this.dayList=dayList;
        }
        public HBaseScanEntry(@JsonProperty("types") ScanType[] types, @JsonProperty("startDay")String startDay, @JsonProperty("endDay")String endDay,
                              @JsonProperty("event") String eventPattern,@JsonProperty("project_id")String pID){
            this.types = types;
            //int size = 0;
            //this.recordSize = size;
            this.eventPattern=eventPattern;
            this.pID=pID;
            this.dayList=new ArrayList<String>();
            try {
                for(String day=startDay;compareDate(day,endDay)<=0;day=calDay(day,1)){
                    dayList.add(day);
                }
            } catch (ParseException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        public String getpID() {
            return pID;
        }

        @Override
        public OperatorCost getCost() {
            return new OperatorCost(1, 2, 1, 1);
        }

        @Override
        public Size getSize() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        public ScanType[] getTypes() {
            return types;
        }

        public String getEventPattern() {
            return eventPattern;
        }
        private int compareDate(String DATE1, String DATE2) throws ParseException {
            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                Date dt1 = df.parse(DATE1);
                Date dt2 = df.parse(DATE2);
                if (dt1.getTime() > dt2.getTime()) {
                    return 1;
                } else if (dt1.getTime() < dt2.getTime()) {
                    return -1;
                } else {
                    return 0;
                }
            } catch (Exception e) {
                //LOG.error("Invalid date format! Date1: " + DATE1 + "\tDate2: " + DATE2, e);
                e.printStackTrace();
                throw new ParseException(DATE1 + "\t" + DATE2, 0);
            }

        }
        private String calDay(String date, int dis) throws ParseException {
            try {
                TimeZone TZ = TimeZone.getTimeZone("GMT+8");
                SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
                Date temp = new Date(getTimestamp(date));

                java.util.Calendar ca = Calendar.getInstance(TZ);
                ca.setTime(temp);
                ca.add(Calendar.DAY_OF_MONTH, dis);
                return df.format(ca.getTime());
            } catch (Exception e) {
                e.printStackTrace();
                //LOG.error("CalDay got exception! " + date + " " + dis);
                throw new ParseException(date + " " + dis, 0);
            }
        }
        private long getTimestamp(String date) {
            String dateString = date + " 00:00:00";
            SimpleDateFormat tdf = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
            Date nowDate = null;
            try {
                nowDate = tdf.parse(dateString);
            } catch (ParseException e) {
                //LOG.error("DateManager.daydis catch Exception with params is "
                //+ date, e);
            }
            if (nowDate != null) {
                return nowDate.getTime();
            } else {
                return -1;
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ScanType{
        @JsonProperty("type") public SchemaDefProtos.MinorType minorType;
        private String name;
        private SchemaDefProtos.DataMode mode;

        @JsonCreator
        public ScanType(@JsonProperty("name") String name, @JsonProperty("type") SchemaDefProtos.MinorType minorType,
                        @JsonProperty("mode") SchemaDefProtos.DataMode mode) {
            this.name = name;
            this.minorType = minorType;
            this.mode = mode;
        }

        @JsonProperty("type")
        public SchemaDefProtos.MinorType getMinorType() {
            return minorType;
        }
        public String getName() {
            return name;
        }
        public SchemaDefProtos.DataMode getMode() {
            return mode;
        }

        @JsonIgnore
        public SchemaDefProtos.MajorType getMajorType(){
            SchemaDefProtos.MajorType.Builder b = SchemaDefProtos.MajorType.newBuilder();
            b.setMode(mode);
            b.setMinorType(minorType);
            return b.build();
        }

    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
        Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

        mappings = new LinkedList[endpoints.size()];

        int i =0;
        for(HBaseScanEntry e : this.getReadEntries()){
            if(i == endpoints.size()) i -= endpoints.size();
            LinkedList<HBaseScanEntry> entries = mappings[i];
            if(entries == null){
                entries = new LinkedList<HBaseScanEntry>();
                mappings[i] = entries;
            }
            entries.add(e);
            i++;
        }
    }


    @Override
    public Scan<?> getSpecificScan(int minorFragmentId) {
        assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
        return new HBaseScanPOP(url, mappings[minorFragmentId]);
    }

    @Override
    @JsonIgnore
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.isEmpty());
        return new HBaseScanPOP(url, readEntries);

    }

}

