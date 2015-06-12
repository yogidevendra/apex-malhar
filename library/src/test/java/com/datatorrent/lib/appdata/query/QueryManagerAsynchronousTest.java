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

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.appdata.dimensions.DimensionsComputationCustomTest.AdInfoResult;
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;

public class QueryManagerAsynchronousTest
{
  @Test
  public void stressTest() throws Exception
  {
    final int totalTuples = 1000;

    AppDataWindowEndQueueManager<MockQuery, Void> queueManager = new AppDataWindowEndQueueManager<MockQuery, Void>();

    DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    CollectorTestSink<AdInfoResult> sink = new CollectorTestSink<AdInfoResult>();
    TestUtils.setSink(outputPort, sink);

    MessageSerializerFactory msf = new MessageSerializerFactory(new ResultFormatter());

    QueryManagerAsynchronous<MockQuery, Void, MutableLong, MockResult> queryManagerAsynch = new
    QueryManagerAsynchronous<MockQuery, Void, MutableLong, MockResult>(outputPort,
                                                                       queueManager,
                                                                       new NOPQueryExecutor(),
                                                                       msf);

    Thread producerThread = new Thread(new ProducerThread(queueManager,
                                                          totalTuples));
    producerThread.start();

    long startTime = System.currentTimeMillis();

    queryManagerAsynch.setup(null);

    for(int numWindows = 0;
        sink.collectedTuples.size() < totalTuples &&
        ((System.currentTimeMillis() - startTime) < 5000);
        numWindows++) {
      queryManagerAsynch.beginWindow(numWindows);
      Thread.sleep(100);
      queryManagerAsynch.endWindow();
    }

    queryManagerAsynch.teardown();

    Assert.assertEquals(totalTuples, sink.collectedTuples.size());
  }

  public static class NOPQueryExecutor implements QueryExecutor<MockQuery, Void, MutableLong, MockResult>
  {
    public NOPQueryExecutor()
    {
    }

    @Override
    public MockResult executeQuery(MockQuery query, Void metaQuery, MutableLong queueContext)
    {
      return new MockResult(query);
    }
  }

  public static class ProducerThread implements Runnable
  {
    private final int totalTuples;
    private AppDataWindowEndQueueManager<MockQuery, Void> queueManager;


    public ProducerThread(AppDataWindowEndQueueManager<MockQuery, Void> queueManager,
                          int totalTuples)
    {
      this.queueManager = queueManager;
      this.totalTuples = totalTuples;
    }

    @Override
    public void run()
    {
      for(int tupleCounter = 0;
          tupleCounter < totalTuples;
          tupleCounter++) {
        queueManager.enqueue(new MockQuery(tupleCounter + ""), null, new MutableLong(1L));
        try {
          Thread.sleep(1);
        }
        catch(InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }
}
