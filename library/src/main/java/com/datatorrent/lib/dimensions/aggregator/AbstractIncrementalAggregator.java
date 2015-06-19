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

package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.Aggregate;
import com.datatorrent.lib.dimensions.Aggregate.EventKey;

public abstract class AbstractIncrementalAggregator<EVENT> implements IncrementalAggregator<EVENT>
{
  private static final long serialVersionUID = 201506180600L;

  private int bucketID;
  private int schemaID;
  private int dimensionDescriptorID;
  private int aggregatorID;
  private FieldsDescriptor keyFieldsDescriptor;
  private FieldsDescriptor valueFieldsDescriptor;
  private GPOGetters keyGetters;
  private GPOGetters valueGetters;

  public AbstractIncrementalAggregator()
  {
  }

  @Override
  public Aggregate getGroup(EVENT src, int aggregatorIndex)
  {
    GPOMutable key = new GPOMutable(keyFieldsDescriptor);
    GPOMutable value = new GPOMutable(valueFieldsDescriptor);

    GPOUtils.copyPOJOToGPO(key, keyGetters, src);
    GPOUtils.copyPOJOToGPO(value, valueGetters, src);

    EventKey eventKey = new EventKey(bucketID,
                                     schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID,
                                     key);

    Aggregate aggregate = new Aggregate(eventKey,
                                        value);
    aggregate.setAggregatorIndex(aggregatorIndex);
    return aggregate;
  }

  /**
   * @return the keyGetters
   */
  public GPOGetters getKeyGetters()
  {
    return keyGetters;
  }

  /**
   * @param keyGetters the keyGetters to set
   */
  public void setKeyGetters(GPOGetters keyGetters)
  {
    this.keyGetters = keyGetters;
  }

  /**
   * @return the valueGetters
   */
  public GPOGetters getValueGetters()
  {
    return valueGetters;
  }

  /**
   * @param valueGetters the valueGetters to set
   */
  public void setValueGetters(GPOGetters valueGetters)
  {
    this.valueGetters = valueGetters;
  }

  /**
   * @return the keyFieldsDescriptor
   */
  public FieldsDescriptor getKeyFieldsDescriptor()
  {
    return keyFieldsDescriptor;
  }

  /**
   * @param keyFieldsDescriptor the keyFieldsDescriptor to set
   */
  public void setKeyFieldsDescriptor(FieldsDescriptor keyFieldsDescriptor)
  {
    this.keyFieldsDescriptor = keyFieldsDescriptor;
  }

  /**
   * @return the valueFieldsDescriptor
   */
  public FieldsDescriptor getValueFieldsDescriptor()
  {
    return valueFieldsDescriptor;
  }

  /**
   * @param valueFieldsDescriptor the valueFieldsDescriptor to set
   */
  public void setValueFieldsDescriptor(FieldsDescriptor valueFieldsDescriptor)
  {
    this.valueFieldsDescriptor = valueFieldsDescriptor;
  }

  /**
   * @return the bucketID
   */
  public int getBucketID()
  {
    return bucketID;
  }

  /**
   * @param bucketID the bucketID to set
   */
  public void setBucketID(int bucketID)
  {
    this.bucketID = bucketID;
  }

  /**
   * @return the schemaID
   */
  public int getSchemaID()
  {
    return schemaID;
  }

  /**
   * @param schemaID the schemaID to set
   */
  public void setSchemaID(int schemaID)
  {
    this.schemaID = schemaID;
  }

  /**
   * @return the dimensionsDescriptorID
   */
  public int getDimensionsDescriptorID()
  {
    return dimensionDescriptorID;
  }

  /**
   * @param dimensionDescriptorID the dimensionsDescriptorID to set
   */
  public void setDimensionsDescriptorID(int dimensionDescriptorID)
  {
    this.dimensionDescriptorID = dimensionDescriptorID;
  }

  /**
   * @return the aggregatorID
   */
  public int getAggregatorID()
  {
    return aggregatorID;
  }

  /**
   * @param aggregatorID the aggregatorID to set
   */
  public void setAggregatorID(int aggregatorID)
  {
    this.aggregatorID = aggregatorID;
  }

  @Override
  public int computeHashCode(Object t)
  {
    return ((Aggregate) t).getEventKey().hashCode();
  }

  @Override
  public boolean equals(Object t, Object t1)
  {
    return ((Aggregate) t).getEventKey().equals(((Aggregate) t1).getEventKey());
  }
}
