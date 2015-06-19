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
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.Aggregate.Aggregate;
import com.datatorrent.lib.dimensions.Aggregate.Aggregate.AggregateHashingStrategy;
import com.datatorrent.lib.dimensions.Aggregate.DimensionsEventDimensionsCombination;
import com.datatorrent.lib.dimensions.Aggregate.InputEvent;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.datatorrent.lib.dimensions.aggregator.IncrementalAggregator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * <p>
 * This is a base class for a generic dimensions computation operator. Generic dimensions computation operators work by taking
 * inputs
 * applying {@link IncrementalAggregators} to inputs. Generic dimensions computation operators work
 * in the following way:<br/>
 * <ol>
 * <li>An {@link AggregatorRegistry} is set on the operator. The {@link AggregatorRegistry} provides implementations of {@link IncrementalAggregator}s
 * which can be applied to inputs.</li>
 * <li>A raw input event is received by the operator. The operator converts the input into an {@link InputEvent} in its {@link #convertInput} method. This
 * is done because {@link IncrementalAggregator}s only understand how to aggregate {@link InputEvent}s.</li>
 * <li>The {@link InputEvent} is sent to an {@link IncrementalAggregator} and the {@link IncrementalAggregator} aggregates input
 * events into {@link Aggregate}s. These {@link Aggregate}s are then emitted by the operator.</li>
 * </ol>
 * </p>
 * <p>
 * The advantages of this implementation of dimensions computation are that it is usable from App Builder,
 * and also does not require aggregators to be reimplemented for different input types like the {@link DimensionsComputationCustom}
 * operator does.
 * </p>
 * <p>
 * The disadvantage of this operator is that it is not as performant as the {@link DimensionsComputationCustom} operator.
 * </p>
 * @param <INPUT> The type of input data received by the operator.
 */
public abstract class AbstractDimensionsComputationFlexible<INPUT> extends AbstractDimensionsComputation<InputEvent, Aggregate>
{
  /**
   * The {@link AggregatorRegistry} to use for this dimensions computation operator.
   */
  protected AggregatorRegistry aggregatorRegistry = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY;
  /**
   * This maps aggregatorIDs provided by the {@link AggregatorRegistry} to aggregate indices, which are used
   * by the dimensions computation unifier to unify aggregates appropriately.
   */
  protected Int2IntOpenHashMap aggregatorIdToAggregateIndex;

  /**
   * The input port which receives events to perform dimensions computation on.
   */
  public transient final DefaultInputPort<INPUT> inputEvent = new DefaultInputPort<INPUT>() {
    @Override
    public void process(INPUT tuple)
    {
      processInputEvent(tuple);
    }
  };

  /**
   * Creates an operator.
   */
  public AbstractDimensionsComputationFlexible()
  {
    //This sets a default unifier hashing strategy which aggregates events with the same {@link EventKey} together in the
    //unifier.
    unifierHashingStrategy = AggregateHashingStrategy.INSTANCE;
  }

  /**
   * This method is called for each input tuple when the input tuple is received.
   * @param tuple The input tuple.
   */
  public abstract void processInputEvent(INPUT tuple);

  /**
   * Converts the given input tuple into the appropriate {@link InputEvent} based off of the given
   * {@link DimensionsConversionContext}. The {@link DimensionsConversionContext} defines the {@link DimensionsDescriptor}
   * that the create {@link InputEvent} will correspond to. It also determines the schemaID and aggregator
   * that applied to the given {@link InputEvent}.
   * @param input The {@link InputEvent} to convert.
   * @param conversionContext The {@link DimensionsConversionContext} to apply to the given input tuple.
   * @return The converted tuple.
   */
  public abstract InputEvent convertInput(INPUT input,
                                          DimensionsConversionContext conversionContext);

  @Override
  @SuppressWarnings({"rawtypes","unchecked"})
  public void setup(OperatorContext context)
  {
    super.setup(context);

    aggregatorRegistry.setup();

    if(maps == null) {
      Map<Integer, IncrementalAggregator> idToAggregator =
      aggregatorRegistry.getIncrementalAggregatorIDToAggregator();

      List<Integer> ids = Lists.newArrayList(idToAggregator.keySet());
      maps = new AggregateMap[ids.size()];
      aggregatorIdToAggregateIndex = new Int2IntOpenHashMap();
      Collections.sort(ids);

      for(int aggregateIndex = 0;
          aggregateIndex < ids.size();
          aggregateIndex++) {
        int aggregatorId = ids.get(aggregateIndex);

        IncrementalAggregator aggregator = idToAggregator.get(aggregatorId);
        AggregateMap<InputEvent, Aggregate> aggregateMap
                = new AggregateMap<InputEvent, Aggregate>(aggregator,
                                                          DimensionsEventDimensionsCombination.INSTANCE,
                                                          aggregateIndex);
        maps[aggregateIndex] = aggregateMap;

        aggregatorIdToAggregateIndex.put(aggregatorId, aggregateIndex);
      }
    }
  }

  @Override
  @VisibleForTesting
  public Aggregator<InputEvent, Aggregate>[] configureDimensionsComputationUnifier()
  {
    aggregatorIdToAggregateIndex = new Int2IntOpenHashMap();

    computeAggregatorIdToAggregateIndex();

    @SuppressWarnings({"unchecked","rawtypes"})
    Aggregator<InputEvent, Aggregate>[] aggregators = new Aggregator[aggregatorIdToAggregateIndex.size()];

    for(Entry<Integer, Integer> entry: aggregatorIdToAggregateIndex.entrySet()) {
      Integer aggregatorId = entry.getKey();
      Integer aggregatorIndex = entry.getValue();

      Aggregator<InputEvent, Aggregate> aggregator =
      aggregatorRegistry.getIncrementalAggregatorIDToAggregator().get(aggregatorId);

      aggregators[aggregatorIndex] = aggregator;
    }

    unifier.setAggregators(aggregators);

    if(this.unifierHashingStrategy != null &&
       unifier.getHashingStrategy() == null) {
      unifier.setHashingStrategy(this.unifierHashingStrategy);
    }

    return aggregators;
  }

  /**
   * This is a helper method which maps the aggregatorIDs of aggregates to a corresponding aggregateIndex.
   * The aggregateIndex is used in the unifier and serves a similar purpose to the aggregatorID, because it
   * also determines what {@link IncrementalAggregator} to apply to an {@link Aggregate}.
   */
  private void computeAggregatorIdToAggregateIndex()
  {
    aggregatorRegistry.setup();

    Map<Integer, IncrementalAggregator> idToAggregator
            = aggregatorRegistry.getIncrementalAggregatorIDToAggregator();

    List<Integer> ids = Lists.newArrayList(idToAggregator.keySet());
    Collections.sort(ids);

    for(int aggregateIndex = 0;
        aggregateIndex < ids.size();
        aggregateIndex++) {
      int aggregatorId = ids.get(aggregateIndex);
      aggregatorIdToAggregateIndex.put(aggregatorId, aggregateIndex);
    }
  }

  /**
   * Returns the {@link AggregatorRegistry} set on the dimensions computation operator.
   * @return The {@link AggregatorRegistry} set on this dimensions computation operator.
   */
  public AggregatorRegistry getAggregatorRegistry()
  {
    return aggregatorRegistry;
  }

  /**
   * Sets the {@link AggregatorRegistry} to use for this operator.
   * @param aggregatorRegistry The {@link AggregatorRegistry} to use for this operator.
   */
  public void setAggregatorRegistry(AggregatorRegistry aggregatorRegistry)
  {
    this.aggregatorRegistry = aggregatorRegistry;
  }

  /**
   * This is a context object that is passed to the {@link #convertInput} method in order to
   * determine the type of {@link InputEvent} that the {@link #convertInput} method should
   * produce.
   */
  public static class DimensionsConversionContext
  {
    /**
     * The schemaID to apply to the {@link InputEvent}.
     */
    public int schemaID;
    /**
     * The aggregatorID of the aggregator to use on the {@link InputEvent}.
     */
    public int aggregatorID;
    /**
     * The dimensions descriptor id to apply to the {@link InputEvent}.
     */
    public int dimensionDescriptorID;
    /**
     * The {@link DimensionsDescriptor} corresponding to the given dimension descriptor id.
     */
    public DimensionsDescriptor dd;
    /**
     * The {@link FieldsDescriptor} for the key of a new {@link InputEvent}.
     */
    public FieldsDescriptor keyFieldsDescriptor;
    /**
     * The {@link FieldsDescriptor} for the aggregate of a new {@link InputEvent}.
     */
    public FieldsDescriptor aggregateDescriptor;

    /**
     * Constructor for creating conversion context.
     */
    public DimensionsConversionContext()
    {
      //Do nothing.
    }
  }
}
