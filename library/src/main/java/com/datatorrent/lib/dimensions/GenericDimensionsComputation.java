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

package com.datatorrent.lib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.dimensions.AbstractDimensionsComputationFlexible.DimensionsConversionContext;
import com.datatorrent.lib.dimensions.Aggregate.Aggregate;
import com.datatorrent.lib.dimensions.Aggregate.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class GenericDimensionsComputation<EVENT> implements Operator
{
  private com.datatorrent.lib.statistics.DimensionsComputation<InputEvent, Aggregate> dimensionsComputation;

  /**
   * The {@link AggregatorRegistry} to use for this dimensions computation operator.
   */
  private AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  /**
   * This maps aggregatorIDs provided by the {@link AggregatorRegistry} to aggregate indices, which are used
   * by the dimensions computation unifier to unify aggregates appropriately.
   */
  protected Int2IntOpenHashMap aggregatorIdToAggregateIndex;

  private transient com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl<InputEvent, Aggregate> unifier;

  /**
   * The output port for the aggregates.
   */
  public final transient DefaultOutputPort<Aggregate> output = new DefaultOutputPort<Aggregate>() {
    @Override
    public Unifier<Aggregate> getUnifier()
    {
      return unifier;
    }
  };

  /**
   * The input port which receives events to perform dimensions computation on.
   */
  public transient final DefaultInputPort<InputEvent> inputEvent = new DefaultInputPort<InputEvent>() {
    @Override
    public void process(InputEvent tuple)
    {
      processInputEvent(tuple);
    }
  };

  public GenericDimensionsComputation()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    dimensionsComputation = new com.datatorrent.lib.statistics.DimensionsComputation<InputEvent, Aggregate>();

    Map<Integer, IncrementalAggregator> idToAggregator = aggregatorRegistry.getIncrementalAggregatorIDToAggregator();

    List<Integer> ids = Lists.newArrayList(idToAggregator.keySet());
    aggregatorIdToAggregateIndex = new Int2IntOpenHashMap();
    Collections.sort(ids);

    IncrementalAggregator[] aggregatorArray = new IncrementalAggregator[ids.size()];

    for(int aggregateIndex = 0;
        aggregateIndex < ids.size();
        aggregateIndex++) {
      int aggregatorId = ids.get(aggregateIndex);
      aggregatorIdToAggregateIndex.put(aggregatorId, aggregateIndex);
      aggregatorArray[aggregateIndex] = idToAggregator.get(aggregatorId);
    }

    unifier = new com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl<InputEvent, Aggregate>();
    unifier.setAggregators(aggregatorArray);
  }

  @Override
  public void beginWindow(long windowId)
  {
    dimensionsComputation.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    dimensionsComputation.endWindow();
  }

  @Override
  public void teardown()
  {
    dimensionsComputation.teardown();
  }

  public abstract void processInputEvent(InputEvent inputEvent);

  /**
   * Converts the given input tuple into the appropriate {@link InputEvent} based off of the given
   * {@link DimensionsConversionContext}. The {@link DimensionsConversionContext} defines the {@link DimensionsDescriptor}
   * that the create {@link InputEvent} will correspond to. It also determines the schemaID and aggregator
   * that applied to the given {@link InputEvent}.
   *
   * @param input The {@link InputEvent} to convert.
   * @param conversionContext The {@link DimensionsConversionContext} to apply to the given input tuple.
   * @return The converted tuple.
   */
  public abstract InputEvent convertInput(EVENT input,
                                          DimensionsConversionContext conversionContext);

  /**
   * @return the aggregatorRegistry
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * @param aggregatorRegistry the aggregatorRegistry to set
   */
  public void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }
}
