/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.query;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.google.common.base.Preconditions;

import java.util.concurrent.Semaphore;

public class QueryManagerAsynchronous<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> implements Component<OperatorContext>{
  private DefaultOutputPort<String> resultPort = null;

  private transient boolean inWindow = false;
  private transient Semaphore inWindowSemaphore = new Semaphore(0);

  private QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager;
  private QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor;

  public QueryManagerAsynchronous(DefaultOutputPort<String> resultPort,
                                  QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager,
                                  QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor)
  {
    setResultPort(resultPort);
    setQueueManager(queueManager);
    setQueryExecutor(queryExecutor);
  }

  private void setResultPort(DefaultOutputPort<String> resultPort)
  {
    this.resultPort = Preconditions.checkNotNull(resultPort);
  }

  private void setQueueManager(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager)
  {
    this.queueManager = Preconditions.checkNotNull(queueManager);
  }

  private void setQueryExecutor(QueryExecutor<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT, RESULT> queryExecutor)
  {
    this.queryExecutor = Preconditions.checkNotNull(queryExecutor);
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  public void beginWindow(long windowID)
  {
    inWindowSemaphore.release();
  }

  public void endWindow()
  {
    try {
      inWindowSemaphore.acquire();
    }
    catch(InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void teardown()
  {
  }

  private class ProcessingThread implements Runnable
  {
    @Override
    public void run()
    {
      //Do this forever
      while(true) {
        //Grab something from the queue as soon as it's available.
        QueryBundle<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queryBundle = queueManager.dequeueBlock();

        try {
          inWindowSemaphore.acquire();
        }
        catch(InterruptedException ex) {
          throw new RuntimeException(ex);
        }

        //We are gauranteed to be in the operator's window now.

        

        //We are done processing the query allow the operator to continue to the next window if it
        //wants to
        inWindowSemaphore.release();
      }
    }
  }
}
