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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.get;
import static org.apache.drill.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static org.apache.drill.exec.proto.UserProtos.RunQuery.newBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ZKClusterCoordinator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.rpc.*;
import org.apache.drill.exec.rpc.BasicClientWithConnection.ServerConnection;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserClient;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms String into ByteBuf
 */
public class DrillClient implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillClient.class);

  DrillConfig config;
  private UserClient client;
  private ClusterCoordinator clusterCoordinator;
  private int reconnectTimes;
  private int reconnectDelay;


  public DrillClient() {
    this(DrillConfig.create());
  }

  public DrillClient(String fileName) {
    this(DrillConfig.create(fileName));
  }

  public DrillClient(DrillConfig config) {
    this(config, null);
  }

  public DrillClient(DrillConfig config, ClusterCoordinator coordinator){
    this.config = config;
    this.clusterCoordinator = coordinator;
    this.reconnectTimes = config.getInt(ExecConstants.BIT_RETRY_TIMES);
    this.reconnectDelay = config.getInt(ExecConstants.BIT_RETRY_DELAY);
  }


  /**
   * Connects the client to a Drillbit server
   *
   * @throws IOException
   */
  public void connect() throws Exception {
    if(clusterCoordinator == null){
      this.clusterCoordinator = new ZKClusterCoordinator(this.config);
      this.clusterCoordinator.start(10000);
    }

    Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
    checkState(!endpoints.isEmpty(), "No DrillbitEndpoint can be found");
    // just use the first endpoint for now
    DrillbitEndpoint endpoint = endpoints.iterator().next();
    ByteBufAllocator bb = new PooledByteBufAllocator(true);
    this.client = new UserClient(bb, new NioEventLoopGroup(1, new NamedThreadFactory("Client-")));
    try {
      logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
      FutureHandler f = new FutureHandler();
      this.client.connect(f, endpoint);
      f.checkedGet();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public boolean isActive(){
    return client.isActive();
  }

  public synchronized boolean  reconnect() {
    if(client.isActive()){
      return true;
    }
    int retry = reconnectTimes;
    while (retry > 0) {
      retry--;
      try {
        Thread.sleep(this.reconnectDelay);
      } catch (Exception e) {
        // do nothing
      }
      Collection<DrillbitEndpoint> endpoints = clusterCoordinator.getAvailableEndpoints();
      if (endpoints.isEmpty()) {
        continue;
      }
      DrillbitEndpoint endpoint = endpoints.iterator().next();
      try {
        FutureHandler f = new FutureHandler();
        client.close();
        this.client.connect(f, endpoint);
        f.checkedGet();
        logger.debug("Reconnection to server{}:{} success", endpoint.getAddress(), endpoint.getUserPort());
        return true;
      } catch (Exception e) {
        logger.error("Reconnection to server{}:{} failed.", endpoint.getAddress(), endpoint.getUserPort());
      }
    }
    return false;
  }

  /**
   * Closes this client's connection to the server
   *
   * @throws IOException
   */
  public void close() throws IOException {
    this.client.close();
  }

  /**
   * Submits a Logical plan for direct execution (bypasses parsing)
   *
   * @param plan the plan to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public List<QueryResultBatch> runQuery(QueryType type, String plan) throws RpcException {
    return runQuery(type, plan, Long.MAX_VALUE);
  }


  /**
   * Submits a Logical plan/physical plan for direct execution, or sql for parsing and execution, and waits for a limited period of time.
   *
   * invocation to this method blocks for at most timeoutMillisec,
   * and returns query result if available.
   * if wait time outs, throws RpcException wrapping TimeoutException.
   * In the case of timeout, the server side query is still being processed.
   *
   * @param type the type of plan, SQL or LOGICAL or PHYSICAL
   * @param plan json for plan, or sql
   * @param timeoutMillisec timeout, in milliseconds.
   * @return query result.
   * @throws RpcException if anything goes wrong, or timed out. see nested exception for detail.
   */
  public List<QueryResultBatch> runQuery(QueryType type, String plan, long timeoutMillisec) throws RpcException {
    return doSubmit(type, plan).getResults(timeoutMillisec);
  }

  /**
   * Submits a Logical plan/physical plan for direct execution, or sql for parsing and execution.
   * Invocation to this method returns immediately after successfully sending requests to drillbit.
   * @param type the type of plan, SQL or LOGICAL or PHYSICAL
   * @param plan json for plan, or sql
   * @return a Future object which can be further queried for execution status or query result.
   * @throws RpcException
   */
  public Future<List<QueryResultBatch>> submitQuery(QueryType type, String plan) throws RpcException{
    return doSubmit(type, plan).future;
  }

  private ListHoldingResultsListener doSubmit(QueryType type, String plan) throws RpcException{
    UserProtos.RunQuery query = newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build();
    ListHoldingResultsListener listener = new ListHoldingResultsListener(query);
    client.submitQuery(listener, query);
    return listener;
  }

  private class ListHoldingResultsListener implements UserResultsListener {
    private final UserProtos.RunQuery query;
    private Vector<QueryResultBatch> results = new Vector<QueryResultBatch>();
    private SettableFuture<List<QueryResultBatch>> future = SettableFuture.create();

    public ListHoldingResultsListener(UserProtos.RunQuery query) {
      this.query = query;
    }


    @Override
    public void submissionFailed(RpcException ex) {
      logger.debug("submissionFailed with exception:{}", ex);      
      if(ex instanceof ChannelClosedException){
        if(reconnect()){
          try {
            logger.debug("resubmitting query...");
            client.submitQuery(this, query);
          } catch (RpcException e) {
            fail(e);
          }
        }else{
          //reconnect failed
          fail(ex);
        }
      } else{
        fail(ex);
      }
    }

    private void fail(Exception e) {
      logger.debug("Submission failed.", e);
      future.setException(e);
    }

    @Override
    public void resultArrived(QueryResultBatch result) {
      logger.debug("Result arrived.  Is Last Chunk: {}.  Full Result: {}", result.getHeader().getIsLastChunk(), result);
      results.add(result);
      if(result.getHeader().getIsLastChunk()){
        future.set(results);
      }
    }

    public List<QueryResultBatch> getResults() throws RpcException{
      return getResults(Long.MAX_VALUE);
    }

    public List<QueryResultBatch> getResults(long timeoutMillisec) throws RpcException{
      try{
        return future.get(timeoutMillisec, TimeUnit.MILLISECONDS);
      }catch(Throwable t){
        throw RpcException.mapException(t);
      }
    }
  }

  private class FutureHandler extends AbstractCheckedFuture<Void, RpcException> implements RpcConnectionHandler<ServerConnection>, DrillRpcFuture<Void>{

    protected FutureHandler() {
      super( SettableFuture.<Void>create());
    }

    @Override
    public void connectionSucceeded(ServerConnection connection) {
      getInner().set(null);
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      getInner().setException(new RpcException(String.format("Failure connecting to server. Failure of type %s.", type.name()), t));
    }

    private SettableFuture<Void> getInner(){
      return (SettableFuture<Void>) delegate();
    }

    @Override
    protected RpcException mapException(Exception e) {
      return RpcException.mapException(e);
    }

    @Override
    public ByteBuf getBuffer() {
      return null;
    }

  }

}
