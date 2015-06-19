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

import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimenDimensionsEventregate.EventKey;
import com.datatorrent.libDimensionsEventns.Aggregate.InputEvent;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * This is a generic dimensions computation operator, which operates on POJOs.
 * </p>
 * <p>
 * <h3>Benchmark Results:</h3><br/>
 * This operator was benchmarked with the following configuration:<br/>
 * <ul>
 * <li><b>Memory:</b> 8.5gb</li>
 * <li>8 {@link DimensionDescriptors}</li>
 * <li>1 aggregator ({@link AggregatorSum})</li>
 * <li>3 key fields</li>
 * <li>4 aggregate fields</li>
 * </ul>
 * <br/>
 * <br/>
 * The operator was able to process 70,000 tuples/sec.
 * </p>
 */
public class DimensionsComputationFlexibleSingleSchemaPOJO extends AbstractDimensionsComputationFlexibleSingleSchema<Object>
{
  /**
   * This is a map from aggregate field name to aggregate getter expressions.
   */
  @NotNull
  private Map<String, String> aggregateToExpression;
  /**
   * This is a map from key field names to key getter expressions.
   */
  @NotNull
  private Map<String, String> keyToExpression;

  /**
   * This holds {@link GPOGetters} which contain the getters used to extract aggregates from
   * an incoming pojo for a particular {@link DimensionsDescriptor} and aggregator combination. This
   * is effectively a map where the first key is the dimensionsDescriptorID, and the second key is
   * the aggregatorID.
   */
  private transient List<Int2ObjectMap<GPOGetters>> dimensionDescriptorIDToAggregatorIDToGetters;
  /**
   * This holds {@link GPOGetters} which contain the getters used to extract key fields from an
   * incoming pojo for a particular {@link DimensionsDescriptor}. This is effectively a map where
   * the first keys is the dimensionsDescriptorID, and the second key is the aggregatorID.
   */
  private transient List<GPOGetters> dimensionDescriptorIDToKeyGetters;
  /**
   * Flag indicating whether or not getters were built.
   */
  private transient boolean builtGetters = false;

  /**
   * Creates the operator.
   */
  public DimensionsComputationFlexibleSingleSchemaPOJO()
  {
    this.unifier = new DimensionsComputationUnifierImpl<InputEvent, Aggregate>();
  }

  @Override
  public InputEvent convertInput(Object input,
                                 DimensionsConversionContext conversionContext)
  {
    buildGetters(input);

    GPOMutable key = new GPOMutable(conversionContext.keyFieldsDescriptor);
    GPOGetters keyGetters =
    dimensionDescriptorIDToKeyGetters.get(conversionContext.dimensionDescriptorID);
    GPOUtils.copyPOJOToGPO(key, keyGetters, input);

    GPOMutable aggregates = new GPOMutable(conversionContext.aggregateDescriptor);
    GPOGetters aggregateGetters =
    dimensionDescriptorIDToAggregatorIDToGetters.get(conversionContext.dimensionDescriptorID).get(conversionContext.aggregatorID);
    GPOUtils.copyPOJOToGPO(aggregates, aggregateGetters, input);

    return new InputEvent(new EventKey(conversionContext.schemaID,
                                       conversionContext.dimensionDescriptorID,
                                       conversionContext.aggregatorID,
                                       key),
                          aggregates);
  }

  /**
   * Returns the map from aggregate field name to aggregate field getter expression.
   * @return The map from aggregate field name to aggregate field getter expression.
   */
  public Map<String, String> getAggregateToExpression()
  {
    return aggregateToExpression;
  }

  /**
   * Sets the map from aggregate field name to aggregate field getter expression.
   * @param aggregateToExpression The map from aggregate field name to aggregate field getter expression.
   */
  public void setAggregateToExpression(Map<String, String> aggregateToExpression)
  {
    this.aggregateToExpression = aggregateToExpression;
  }

  /**
   * Returns the map from key field name to key field getter expression.
   * @return The map from key field name to key field getter expression.
   */
  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  /**
   * Sets the map from key field name to key field getter expression.
   * @param keyToExpression The map from key field name to key field getter expression.
   */
  public void setKeyToExpression(Map<String, String> keyToExpression)
  {
    this.keyToExpression = keyToExpression;
  }

  /**
   * This is a helper method which builds the getter methods to extract values from incoming pojos.
   * @param inputEvent An incoming pojo.
   */
  private void buildGetters(Object inputEvent)
  {
    if(builtGetters) {
      return;
    }

    builtGetters = true;

    Class<?> clazz = inputEvent.getClass();

    dimensionDescriptorIDToKeyGetters = Lists.newArrayList();
    List<FieldsDescriptor> keyDescriptors = configurationSchema.getDimensionsDescriptorIDToKeyDescriptor();
    dimensionDescriptorIDToAggregatorIDToGetters = Lists.newArrayList();

    for(int ddID = 0;
        ddID < configurationSchema.getDimensionsDescriptorIDToKeyDescriptor().size();
        ddID++) {
      DimensionsDescriptor dd = configurationSchema.getDimensionsDescriptorIDToDimensionsDescriptor().get(ddID);
      FieldsDescriptor keyDescriptor = keyDescriptors.get(ddID);
      GPOGetters keyGPOGetters = buildGettersForDimensionsDescriptor(clazz, keyDescriptor, dd, keyToExpression, true);
      dimensionDescriptorIDToKeyGetters.add(keyGPOGetters);
      Int2ObjectMap<FieldsDescriptor> map = configurationSchema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().get(ddID);
      IntArrayList aggIDList = configurationSchema.getDimensionsDescriptorIDToAggregatorIDs().get(ddID);
      Int2ObjectOpenHashMap<GPOGetters> aggIDToGetters = new Int2ObjectOpenHashMap<GPOGetters>();
      dimensionDescriptorIDToAggregatorIDToGetters.add(aggIDToGetters);

      for(int aggIDIndex = 0;
          aggIDIndex < aggIDList.size();
          aggIDIndex++) {
        int aggID = aggIDList.get(aggIDIndex);
        FieldsDescriptor aggFieldsDescriptor = map.get(aggID);
        GPOGetters aggGPOGetters = buildGettersForDimensionsDescriptor(clazz, aggFieldsDescriptor, dd, aggregateToExpression, false);
        aggIDToGetters.put(aggID, aggGPOGetters);
      }
    }
  }

  /**
   * This method builds the getters for a particular dimensions descriptor.
   * @param clazz The {@link Class} object for the incoming POJO.
   * @param fieldsDescriptor This is the {@link FieldsDescriptor} object describing the name of types of the fields
   * for which to create getters.
   * @param dd The dimensions descriptor under which these getters will be constructed.
   * @param valueToExpression This is a map from a field's name to its corresponding getter expression.
   * @param isKey True if the constructed getters will be used to extract key values from a pojo.
   * False if the constructed getters will be used to extract aggregates from a pojo.
   * @return The set of getters used to extract either keys or aggregates from the pojo.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private GPOGetters buildGettersForDimensionsDescriptor(Class<?> clazz,
                                                         FieldsDescriptor fieldsDescriptor,
                                                         DimensionsDescriptor dd,
                                                         Map<String, String> valueToExpression,
                                                         boolean isKey)
  {
    GPOGetters gpoGetters = new GPOGetters();

    Map<Type, List<String>> typeToFields = fieldsDescriptor.getTypeToFields();

    for(Map.Entry<Type, List<String>> entry: typeToFields.entrySet()) {
      Type inputType = entry.getKey();
      List<String> fields = entry.getValue();

      switch(inputType) {
        case BOOLEAN: {
          gpoGetters.gettersBoolean = GPOUtils.createGetterBoolean(fields,
                                                                   valueToExpression,
                                                                   clazz);

          break;
        }
        case STRING: {
          gpoGetters.gettersString = GPOUtils.createGetterString(fields,
                                                                 valueToExpression,
                                                                 clazz);

          break;
        }
        case CHAR: {
          gpoGetters.gettersChar = GPOUtils.createGetterChar(fields,
                                                             valueToExpression,
                                                             clazz);

          break;
        }
        case DOUBLE: {
          gpoGetters.gettersDouble = GPOUtils.createGetterDouble(fields,
                                                                 valueToExpression,
                                                                 clazz);

          break;
        }
        case FLOAT: {
          gpoGetters.gettersFloat = GPOUtils.createGetterFloat(fields,
                                                               valueToExpression,
                                                               clazz);
          break;
        }
        case LONG: {
          gpoGetters.gettersLong = new GetterLong[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);
            GetterLong tempGetterLong = PojoUtils.createGetterLong(clazz, valueToExpression.get(field));

            if(isKey && field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
              //Create the special getter for the timestamp
              gpoGetters.gettersLong[getterIndex] = new GetTime(tempGetterLong, dd.getTimeBucket());
            }
            else {
              gpoGetters.gettersLong[getterIndex] = tempGetterLong;
            }
          }

          break;
        }
        case INTEGER: {
          gpoGetters.gettersInteger = new GetterInt[fields.size()];

          for(int getterIndex = 0;
              getterIndex < fields.size();
              getterIndex++) {
            String field = fields.get(getterIndex);

            if(isKey && field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
              //create the special getter for the time bucket
              gpoGetters.gettersInteger[getterIndex]
                      = new GetTimeBucket(dd.getTimeBucket());

            }
            else {
              gpoGetters.gettersInteger[getterIndex]
                      = PojoUtils.createGetterInt(clazz, valueToExpression.get(field));
            }
          }

          break;
        }
        case SHORT: {
          gpoGetters.gettersShort = GPOUtils.createGetterShort(fields,
                                                               valueToExpression,
                                                               clazz);

          break;
        }
        case BYTE: {
          gpoGetters.gettersByte = GPOUtils.createGetterByte(fields,
                                                             valueToExpression,
                                                             clazz);

          break;
        }
        case OBJECT: {
          gpoGetters.gettersObject = GPOUtils.createGetterObject(fields,
                                                                 valueToExpression,
                                                                 clazz);

          break;
        }
        default: {
          throw new IllegalArgumentException("The type " + inputType + " is not supported.");
        }
      }
    }

    return gpoGetters;
  }

  /**
   * <p>
   * This is a special getter to get the time bucket from an incoming POJO.
   * </p>
   */
  public static class GetTimeBucket implements PojoUtils.GetterInt<Object>
  {
    /**
     * The timeBucket to return from this getter.
     */
    private final int timeBucket;

    /**
     * Creates a getter which returns the id of the given {@link TimeBucket}.
     * @param timeBucket  The id of the given {@link TimeBucket}.
     */
    public GetTimeBucket(TimeBucket timeBucket)
    {
      this.timeBucket = timeBucket.ordinal();
    }

    @Override
    public int get(Object obj)
    {
      return timeBucket;
    }
  }

  /**
   * This is a special getter gets the correctly rounded timestamp from the
   * given POJO.
   */
  public static class GetTime implements PojoUtils.GetterLong<Object>
  {
    /**
     * The getter which retrieves the timestamp from the input pojo.
     */
    private final GetterLong<Object> timeGetter;
    /**
     * The {@link TimeBucket} to use in order to round down a retrieved timestamp.
     */
    private final TimeBucket timeBucket;

    /**
     * Creates a getter which retrieves a rounded down timestamp.
     * @param timeGetter The getter which retrieves a timestamp from an input pojo.
     * @param timeBucket The {@link TimeBucket} to use when rounding timestamps.
     */
    public GetTime(GetterLong<Object> timeGetter, TimeBucket timeBucket)
    {
      this.timeGetter = Preconditions.checkNotNull(timeGetter);
      this.timeBucket = Preconditions.checkNotNull(timeBucket);
    }

    @Override
    public long get(Object obj)
    {
      long time = timeGetter.get(obj);
      return timeBucket.roundDown(time);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionsComputationFlexibleSingleSchemaPOJO.class);
}
