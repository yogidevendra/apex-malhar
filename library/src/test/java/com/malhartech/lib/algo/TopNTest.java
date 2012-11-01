/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.dag.TestSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.TopN}<p>
 *
 */

public class TopNTest
{
  private static Logger log = LoggerFactory.getLogger(TopNTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new TopN<String, Integer>());
    testNodeProcessingSchema(new TopN<String, Double>());
    testNodeProcessingSchema(new TopN<String, Float>());
    testNodeProcessingSchema(new TopN<String, Short>());
    testNodeProcessingSchema(new TopN<String, Long>());
  }

  public void testNodeProcessingSchema(TopN oper)
  {
    TestSink<HashMap<String, Number>> sortSink = new TestSink<HashMap<String, Number>>();
    oper.top.setSink(sortSink);
    oper.setN(3);

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();

    input.put("a", 2);
    oper.data.process(input);

    input.clear();
    input.put("a", 20);
    oper.data.process(input);

    input.clear();
    input.put("a", 1000);
    oper.data.process(input);

    input.clear();
    input.put("a", 5);
    oper.data.process(input);

    input.clear();
    input.put("a", 20);
    input.put("b", 33);
    oper.data.process(input);

    input.clear();
    input.put("a", 33);
    input.put("b", 34);
    oper.data.process(input);

    input.clear();
    input.put("b", 34);
    input.put("a", 1);
    oper.data.process(input);

    input.clear();
    input.put("b", 6);
    input.put("a", 1001);
    oper.data.process(input);

    input.clear();
    input.put("c", 9);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, sortSink.collectedTuples.size());
    for (Object o: sortSink.collectedTuples) {
      for (Map.Entry<String, ArrayList<Number>> e: ((HashMap<String, ArrayList<Number>>)o).entrySet()) {
        if (e.getKey().equals("a")) {
          Assert.assertEquals("emitted value for 'a' was ", 3, e.getValue().size());
        }
        else if (e.getKey().equals("b")) {
          Assert.assertEquals("emitted tuple for 'b' was ", 3, e.getValue().size());
        }
        else if (e.getKey().equals("c")) {
          Assert.assertEquals("emitted tuple for 'c' was ", 1, e.getValue().size());
        }
        log.debug(String.format("Sorted list for %s:", e.getKey()));
        for (Number i: e.getValue()) {
          log.debug(String.format("%s", i.toString()));
        }
      }
    }
    log.debug("Done testing round\n");
  }
}
