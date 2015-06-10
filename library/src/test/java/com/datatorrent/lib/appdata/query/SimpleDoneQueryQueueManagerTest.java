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

import com.datatorrent.lib.appdata.schemas.Query;
import com.datatorrent.lib.appdata.query.QueryBundle;
import com.datatorrent.lib.appdata.query.SimpleDoneQueueManager;
import jline.internal.Preconditions;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Assert;
import org.junit.Test;

public class SimpleDoneQueryQueueManagerTest
{
  @Test
  public void firstDoneTest()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(true));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();

    Assert.assertEquals("Should return back null.", null, qb);
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeueBlock()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Assert.assertEquals(1, sdqqm.getNumLeft());

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());

    Assert.assertEquals("Should return same query.", query, qb.getQuery());

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleBlockingTest() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    testBlocking(sdqqm);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeue()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleEnqueueDequeueThenBlock() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Assert.assertEquals(1, sdqqm.getNumLeft());

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());

    testBlocking(sdqqm);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleExpire1()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb.getQueueContext().setValue(true);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void resetPermitsTest() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    Assert.assertEquals(1, sdqqm.getNumLeft());

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(1);

    Assert.assertEquals(1, sdqqm.getNumLeft());

    qb = sdqqm.dequeueBlock();

    Assert.assertEquals(0, sdqqm.getNumLeft());

    testBlocking(sdqqm);

    sdqqm.endWindow();
  }

  @Test
  public void expiredTestBlocking() throws Exception
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    MutableBoolean queueContext = new MutableBoolean(false);
    sdqqm.enqueue(query, null, queueContext);

    Assert.assertEquals(1, sdqqm.getNumLeft());
    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeueBlock();
    Assert.assertEquals(0, sdqqm.getNumLeft());

    sdqqm.endWindow();

    sdqqm.beginWindow(1);

    Assert.assertEquals(1, sdqqm.getNumLeft());

    queueContext.setValue(true);
    testBlocking(sdqqm);

    Assert.assertEquals(0, sdqqm.getNumLeft());

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  @Test
  public void simpleExpire1ThenBlock()
  {
    SimpleDoneQueueManager<Query, Void> sdqqm = new SimpleDoneQueueManager<Query, Void>();

    sdqqm.setup(null);
    sdqqm.beginWindow(0);

    Query query = new MockQuery("1");
    sdqqm.enqueue(query, null, new MutableBoolean(false));

    QueryBundle<Query, Void, MutableBoolean> qb = sdqqm.dequeue();

    Assert.assertEquals("Should return same query.", query, qb.getQuery());
    qb.getQueueContext().setValue(true);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.beginWindow(1);

    qb = sdqqm.dequeue();
    Assert.assertEquals("Should return back null.", null, qb);

    sdqqm.endWindow();
    sdqqm.teardown();
  }

  private void testBlocking(SimpleDoneQueueManager<Query, Void> sdqqm) throws InterruptedException
  {
    Thread thread = new Thread(new BlockedThread<Query, Void, MutableBoolean>(sdqqm));
    thread.start();
    Thread.sleep(100);

    Assert.assertEquals(Thread.State.WAITING, thread.getState());

    thread.stop();
  }

  public static class BlockedThread<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> implements Runnable
  {
    QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager;

    public BlockedThread(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager)
    {
      setQueueManager(queueManager);
    }

    private void setQueueManager(QueueManager<QUERY_TYPE, META_QUERY, QUEUE_CONTEXT> queueManager)
    {
      this.queueManager = Preconditions.checkNotNull(queueManager);
    }

    @Override
    public void run()
    {
      queueManager.dequeueBlock();
    }
  }
}
