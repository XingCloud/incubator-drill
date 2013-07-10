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
package org.apache.drill.exec.physical.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.config.*;
import org.apache.drill.exec.record.RecordBatch;

import java.util.Collections;
import java.util.List;

public class ImplCreator extends AbstractPhysicalVisitor<RecordBatch, FragmentContext, ExecutionSetupException>{
  //static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImplCreator.class);

  private MockScanBatchCreator msc = new MockScanBatchCreator();
  private ScreenCreator sc = new ScreenCreator();
  private RandomReceiverCreator rrc = new RandomReceiverCreator();
  private SingleSenderCreator ssc = new SingleSenderCreator();
  private RootExec root = null;
  
  private ImplCreator(){}
  
  public RootExec getRoot(){
    return root;
  }
  
  
  @Override
  public RecordBatch visitScan(Scan<?> scan, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkNotNull(scan);
    Preconditions.checkNotNull(context);
    
    if(scan instanceof MockScanPOP){
        /*
        HBaseScanPOP.ScanType[] types=new HBaseScanPOP.ScanType[4];
        types[0]=new HBaseScanPOP.ScanType("day", SchemaDefProtos.MinorType.DATE, SchemaDefProtos.DataMode.REQUIRED);
        types[1]=new HBaseScanPOP.ScanType("event", SchemaDefProtos.MinorType.VARCHAR4, SchemaDefProtos.DataMode.REQUIRED);
        types[2]=new HBaseScanPOP.ScanType("uid", SchemaDefProtos.MinorType.INT, SchemaDefProtos.DataMode.REQUIRED);
        types[3]=new HBaseScanPOP.ScanType("val", SchemaDefProtos.MinorType.UINT8, SchemaDefProtos.DataMode.REQUIRED);
        byte[] srk= Bytes.toBytes("20020101click.shutdowm.\\xFF\\x15\\x00K\\x91\\x18");
        byte[] enk=Bytes.toBytes("20020101exit.\\xFF2\\x00\\x15\\x95\\xC6");
        HBaseScanPOP.HBaseScanEntry entry=new HBaseScanPOP.HBaseScanEntry(100,types,srk,enk,"sof-dsk_deu");
        HBaseRecordReader reader=new HBaseRecordReader(entry,null);
        List<RecordReader> readerList=new ArrayList<RecordReader>();
        readerList.add(reader);
        Iterator<RecordReader> iter=readerList.iterator();
        ScanBatch batch=new ScanBatch(context,iter);
        return batch;*/
      return msc.getBatch(context, (MockScanPOP) scan, Collections.<RecordBatch> emptyList());
    }else{
      return super.visitScan(scan, context);  
    }
    
  }

  @Override
  public RecordBatch visitScreen(Screen op, FragmentContext context) throws ExecutionSetupException {
    Preconditions.checkArgument(root == null);
    root = sc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  
  
  @Override
  public RecordBatch visitSingleSender(SingleSender op, FragmentContext context) throws ExecutionSetupException {
    root = ssc.getRoot(context, op, getChildren(op, context));
    return null;
  }

  @Override
  public RecordBatch visitRandomReceiver(RandomReceiver op, FragmentContext context) throws ExecutionSetupException {
    return rrc.getBatch(context, op, null);
  }

  private List<RecordBatch> getChildren(PhysicalOperator op, FragmentContext context) throws ExecutionSetupException{
    List<RecordBatch> children = Lists.newArrayList();
    for(PhysicalOperator child : op){
      children.add(child.accept(this, context));
    }
    return children;
  }
  
  public static RootExec getExec(FragmentContext context, FragmentRoot root) throws ExecutionSetupException{
    ImplCreator i = new ImplCreator();
    root.accept(i, context);
    if(i.root == null) throw new ExecutionSetupException("The provided fragment did not have a root node that correctly created a RootExec value.");
    return i.getRoot();
  }
}
