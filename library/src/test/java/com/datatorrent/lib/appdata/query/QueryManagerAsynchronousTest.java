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
import com.datatorrent.lib.appdata.query.serde.MessageSerializerFactory;
import com.datatorrent.lib.appdata.schemas.ResultFormatter;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class QueryManagerAsynchronousTest
{
  @Test
  public void stressTest() throws Exception
  {
    final int totalTuples = 100000;
    final int batchSize = 100;
    final double waitMillisProb = .01;

    AppDataWindowEndQueueManager<MockQuery, Void> queueManager = new AppDataWindowEndQueueManager<MockQuery, Void>();

    DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    CollectorTestSink<MockResult> sink = new CollectorTestSink<MockResult>();
    TestUtils.setSink(outputPort, sink);

    MessageSerializerFactory msf = new MessageSerializerFactory(new ResultFormatter());

    QueryManagerAsynchronous<MockQuery, Void, MutableLong, MockResult> queryManagerAsynch = new
    QueryManagerAsynchronous<MockQuery, Void, MutableLong, MockResult>(outputPort,
                                                                       queueManager,
                                                                       new NOPQueryExecutor(waitMillisProb),
                                                                       msf);

    Thread producerThread = new Thread(new ProducerThread(queueManager,
                                                          totalTuples,
                                                          batchSize,
                                                          waitMillisProb));
    producerThread.start();
    producerThread.setName("Producer Thread");

    long startTime = System.currentTimeMillis();

    queryManagerAsynch.setup(null);

    int numWindows = 0;
    for(;
        sink.collectedTuples.size() < totalTuples
        && ((System.currentTimeMillis() - startTime) < 60000)
        ;numWindows++) {
      queryManagerAsynch.beginWindow(numWindows);
      Thread.sleep(100);
      queryManagerAsynch.endWindow();
    }

    queryManagerAsynch.teardown();

    LOG.debug("Num windows: {} num tuples: {}", numWindows, sink.collectedTuples.size());
    Assert.assertEquals(totalTuples, sink.collectedTuples.size());
  }

  public static class NOPQueryExecutor implements QueryExecutor<MockQuery, Void, MutableLong, MockResult>
  {
    private final double waitMillisProb;
    private final Random rand = new Random();

    public NOPQueryExecutor(double waitMillisProb)
    {
      this.waitMillisProb = waitMillisProb;
    }

    @Override
    public MockResult executeQuery(MockQuery query, Void metaQuery, MutableLong queueContext)
    {
      if(rand.nextDouble() < waitMillisProb) {
        try {
          Thread.sleep(1);
        }
        catch(InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }

      return new MockResult(query);
    }
  }

  public static class ProducerThread implements Runnable
  {
    private final int totalTuples;
    private final int batchSize;
    private final AppDataWindowEndQueueManager<MockQuery, Void> queueManager;
    private final double waitMillisProb;
    private final Random rand = new Random();

    public ProducerThread(AppDataWindowEndQueueManager<MockQuery, Void> queueManager,
                          int totalTuples,
                          int batchSize,
                          double waitMillisProb)
    {
      this.queueManager = queueManager;
      this.totalTuples = totalTuples;
      this.batchSize = batchSize;
      this.waitMillisProb = waitMillisProb;
    }

    @Override
    public void run()
    {
      int numLoops = totalTuples / batchSize;

      for(int loopCounter = 0, tupleCounter = 0;
          loopCounter < numLoops;
          loopCounter++, tupleCounter++) {
        for(int batchCounter = 0;
            batchCounter < batchSize;
            batchCounter++,
            tupleCounter++) {
          queueManager.enqueue(new MockQuery(tupleCounter + ""), null, new MutableLong(1L));

          if(rand.nextDouble() < waitMillisProb) {
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
  }

  private static final Logger LOG = LoggerFactory.getLogger(QueryManagerAsynchronousTest.class);
}
