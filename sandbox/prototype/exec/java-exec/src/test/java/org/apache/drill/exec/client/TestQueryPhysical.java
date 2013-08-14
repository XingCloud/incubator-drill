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
package org.apache.drill.exec.client;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class TestQueryPhysical extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestQueryPhysical.class);

  @Test
  public void commonQuery() throws  Exception{
     submitQuery("/physical_test1.json");
  }

  @Test
  public void groupByEvent() throws Exception{
    submitQuery("/physical_test2.json");
  }

  @Test
  public void groupByUser() throws Exception{
    submitQuery("/physical_test3.json");
  }

  @Test
  public void filterAndProject() throws Exception{
    submitQuery("/physical_test4.json");
  }

  @Test
  public void uion() throws Exception{
    submitQuery("/physical_test5.json");
  }


  private void submitQuery(String testFile) throws  Exception{
    submitQuery(testFile,QueryType.PHYSICAL);
  }


  public static void submitQuery(String testFile,QueryType queryType) throws Exception {
    try(
        DrillClient client = new DrillClient();){

      // run query.
      client.connect();
      Future<List<QueryResultBatch>> results = client.submitQuery(queryType, Files.toString(FileUtils.getResourceAsFile(testFile), Charsets.UTF_8));

      // look at records
      RecordBatchLoader batchLoader = new RecordBatchLoader(BufferAllocator.getAllocator(CONFIG));
      int recordCount = 0;
      for (QueryResultBatch batch : results.get()) {
        if(batch.getHeader().getQueryState()== UserProtos.QueryResult.QueryState.FAILED){
          System.out.println(batch.getHeader().getError(0).getMessage());
          continue;
        }
        if(!batch.hasData()) continue;
        boolean schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
        boolean firstColumn = true;

        // print headers.
        if (schemaChanged) {
          System.out.println("\n\n========NEW SCHEMA=========\n\n");
          for (ValueVector value : batchLoader) {

            if (firstColumn) {
              firstColumn = false;
            } else {
              System.out.print("\t");
            }
            System.out.print(value.getField().getName());
            System.out.print("[");
            System.out.print(value.getField().getType().getMinorType());
            System.out.print("]");
          }
          System.out.println();
        }


        for (int i = 0; i < batchLoader.getRecordCount(); i++) {
          boolean first = true;
          recordCount++;
          for (ValueVector value : batchLoader) {
            if (first) {
              first = false;
            } else {
              System.out.print("\t");
            }
            Object obj = value.getAccessor().getObject(i);
            if(obj instanceof  byte[]) {
              obj = new String((byte[]) obj) ;
            }
            System.out.print(obj);
          }
          if(!first) System.out.println();
        }



      }
      logger.debug("Received results {}", results);
      //assertEquals(recordCount, 200);
    }
  }

}
