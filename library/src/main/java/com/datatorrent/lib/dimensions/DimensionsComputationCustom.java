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
import com.datatorrent.lib.dimensions.AbstractDimensionsComputation.AggregateMap;
import com.datatorrent.lib.dimensions.DimensionsComputation.UnifiableAggregate;
import com.datatorrent.lib.dimensions.aggregator.Aggregator;
import com.google.common.collect.Lists;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This is a non generic implementation of dimensions computation. This implementation of dimensions computation requires hard coded
 * implementations of the {@link DimensionsCombination} and {@link Aggregator} interfaces, which work with the
 * specified EVENT and AGGREGATE types. The {@link DimensionsCombination}s and {@link Aggregators} used by this operator are defined by
 * setting two properties:
 * <ul>
 * <li><b>dimensionsCombinations:</b> This property defines the {@link DimensionsCombination}s and their corresponding names.</li>
 * <li><b>aggregators:</b> This property defines the {@link Aggregator}s to apply for each {@link DimensionsCombination}.</li>
 * </ul>
 * </p>
 * <p>
 * <h3>Benchmark Results:</h3><br/>
 * This operator was benchmarked with the following configuration:<br/>
 * <ul>
 * <li><b>Memory:</b> 8.5gb</li>
 * <li>8 {@link DimensionDescriptors}</li>
 * <li>1 aggregator (Sum)</li>
 * <li>3 key fields</li>
 * <li>4 aggregate fields</li>
 * </ul>
 * <br/>
 * <br/>
 * The operator was able to process roughly 1,500,000 tuples/sec.
 * </p>
 * @param <EVENT> The type of input events processed by this operator.
 * @param <AGGREGATE> The type of the aggregates emitted by this operator.
 */
public class DimensionsComputationCustom<EVENT, AGGREGATE extends UnifiableAggregate> extends AbstractDimensionsComputation<EVENT, AGGREGATE>
{
  /**
   * <p>
   * This represents the {@link DimensionsCombination}s used by the operator. Each {@link DimensionsCombination} is
   * given a name, which is used to identify what aggregations to apply to a specific {@link DimensionsCombination}.
   * </p>
   * <p>
   * <b>Note:</b> This is a {@link LinkedHashMap} because the iteration order of the map is used to determine the aggregateIndex
   * assigned to the {@link UnifiableAggregate}s emitted by this operator, and the {@link LinkedHashMap} is the only map which
   * gaurantees a consistent iteration order.
   * </p>
   */
  @NotNull
  private LinkedHashMap<String, DimensionsCombination<EVENT, AGGREGATE>> dimensionsCombinations;
  /**
   * <p>
   * This map defines the aggregations that are performed for each {@link DimensionsCombination}. Each key in the
   * map is the name of a {@link DimensionsCombination} as defined in {@link #setDimensionsCombinations}. The value
   * in the map represents the {@link Aggregator}s to use for that {@link DimensionsCombination}.
   * </p>
   * <p>
   * <b>Note:</b> The order of {@link Aggregator}s in the map determines the aggregateIndex assigned to {@link UnifiableAggregate}s
   * emitted by this operator.
   * </p>
   */
  @NotNull
  private Map<String, List<Aggregator<EVENT, AGGREGATE>>> aggregators;

  /**
   * Input data port that takes an event.
   */
  public final transient DefaultInputPort<EVENT> data = new DefaultInputPort<EVENT>(){

    @Override
    public void process(EVENT tuple)
    {
      processInputTuple(tuple);
    }
  };

  /**
   * Creates a new {@link DimensionsComputationCustom} object.
   */
  public DimensionsComputationCustom()
  {
    //This uses a direct hashing strategy which means the unifier will use the AGGREGATE
    //object's equals and hashCode methods to determine what aggregates to unify.
    unifierHashingStrategy = new DirectDimensionsCombination<AGGREGATE, AGGREGATE>();
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public void setup(OperatorContext context)
  {
    int size = computeNumAggregators();

    if(this.maps == null) {
      maps = new AggregateMap[size];
      int aggregateIndex = 0;
      List<String> combinationNames = Lists.newArrayList();
      combinationNames.addAll(dimensionsCombinations.keySet());
      Collections.sort(combinationNames);

      for(String combinationName: combinationNames) {
        DimensionsCombination<EVENT, AGGREGATE> combination = dimensionsCombinations.get(combinationName);
        List<Aggregator<EVENT, AGGREGATE>> tempAggregators = aggregators.get(combinationName);

        for(Aggregator<EVENT, AGGREGATE> aggregator: tempAggregators) {
          maps[aggregateIndex] = new AggregateMap<EVENT, AGGREGATE>(aggregator,
                                                                    combination,
                                                                    aggregateIndex);
          aggregateIndex++;
        }
      }
    }
  }

  /**
   * This method processes each input tuple, and aggregates each tuple to the aggregate stored
   * for each {@link Aggregator} and {@link DimensionsCombination} pair.
   * @param tuple The input tuple to aggregate.
   */
  protected void processInputTuple(EVENT tuple)
  {
    for (int i = 0; i < this.maps.length; i++) {
      maps[i].aggregate(tuple);
    }
  }

  /**
   * Returns a map containing the {@link DimensionsCombination}s applied to data input to this operator. The keys of
   * the map are the names of the {@link DimensionsCombination}s and the values are the {@link DimensionsCombination}s
   * themselves.
   * @return A map containing the {@link DimensionsCombination}s applied to data input to this operator.
   */
  public LinkedHashMap<String, DimensionsCombination<EVENT, AGGREGATE>> getDimensionsCombinations()
  {
    return dimensionsCombinations;
  }

  /**
   * Sets a map containing the {@link DimensionsCombination}s applied to data input to this operator.
   * @param dimensionsCombinations The map containing the {@link DimensionsCombination}s applied to data input to this operator.
   */
  public void setDimensionsCombinations(LinkedHashMap<String, DimensionsCombination<EVENT, AGGREGATE>> dimensionsCombinations)
  {
    this.dimensionsCombinations = dimensionsCombinations;
  }

  /**
   * Returns a map containing the aggregators to apply to each {@link DimensionsCombination}. The key of the
   * map is the name of a {@link DimensionsCombination} as defined in {@link #setDimensionsCombinations}.
   * @return A map containing the aggregators to apply to each {@link DimensionsCombination}.
   */
  public Map<String, List<Aggregator<EVENT, AGGREGATE>>> getAggregators()
  {
    return aggregators;
  }

  /**
   * Sets a map containing the {@link Aggregator}s to be applied to the data input to this operator.
   * @param aggregators A map containing the {@link Aggregator}s to be applied to the data input to this operator.
   */
  public void setAggregators(LinkedHashMap<String, List<Aggregator<EVENT, AGGREGATE>>> aggregators)
  {
    this.aggregators = aggregators;
  }

  @Override
  public Aggregator<EVENT, AGGREGATE>[] configureDimensionsComputationUnifier()
  {
    int numAggregators = computeNumAggregators();
    @SuppressWarnings({"unchecked","rawtypes"})
    Aggregator<EVENT, AGGREGATE>[] aggregatorsArray = new Aggregator[numAggregators];

    int aggregateIndex = 0;
    List<String> combinationNames = Lists.newArrayList();
    combinationNames.addAll(dimensionsCombinations.keySet());
    Collections.sort(combinationNames);

    for(String combinationName: combinationNames) {
      List<Aggregator<EVENT, AGGREGATE>> tempAggregators = aggregators.get(combinationName);

      for(Aggregator<EVENT, AGGREGATE> aggregator: tempAggregators) {
        aggregatorsArray[aggregateIndex] = aggregator;
        aggregateIndex++;
      }
    }

    unifier.setAggregators(aggregatorsArray);

    if(this.unifierHashingStrategy != null &&
       unifier.getHashingStrategy() == null) {
      unifier.setHashingStrategy(this.unifierHashingStrategy);
    }

    return aggregatorsArray;
  }

  /**
   * This is a helper method which computes the number of {@link DimensionsDescriptor} and {@link Aggregator}
   * combinations.
   * @return The number of {@link DimensionsDescriptor} and {@link Aggregator} combinations.
   */
  private int computeNumAggregators()
  {
    int size = 0;

    for(Map.Entry<String, List<Aggregator<EVENT, AGGREGATE>>> entry: aggregators.entrySet()) {
      size += entry.getValue().size();
    }

    return size;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationCustom.class);
}
