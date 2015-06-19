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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;

import java.util.Arrays;

/**
 * This {@link IncrementalAggregator} performs a sum operation over the fields in the given {@link InputEvent}.
 * @param <EVENT> This is the type of the input event.
 */
public class AggregatorSum<EVENT> extends AbstractIncrementalAggregator<EVENT>
{
  private static final long serialVersionUID = 20154301649L;

  public AggregatorSum()
  {
    //Do nothing
  }

  @Override
  public DimensionsEvent getGroup(EVENT src, int aggregatorIndex)
  {
    GPOMutable key = new GPOMutable(keyFieldsDescriptor);
    GPOMutable value = new GPOMutable(valueFieldsDescriptor);

    if(value.getFieldsByte() != null) {
      Arrays.fill(value.getFieldsByte(), (byte) 0);
    }

    if(value.getFieldsShort() != null) {
      Arrays.fill(value.getFieldsShort(), (short) 0);
    }

    if(value.getFieldsInteger() != null) {
      Arrays.fill(value.getFieldsInteger(), 0);
    }

    if(value.getFieldsLong() != null) {
      Arrays.fill(value.getFieldsLong(), 0L);
    }

    if(value.getFieldsFloat() != null) {
      Arrays.fill(value.getFieldsFloat(), 0.0f);
    }

    if(value.getFieldsDouble() != null) {
      Arrays.fill(value.getFieldsDouble(), 0.0);
    }

    EventKey eventKey = new EventKey(bucketID,
                                     schemaID,
                                     dimensionDescriptorID,
                                     aggregatorID,
                                     key);

    DimensionsEvent aggregate = new DimensionsEvent(eventKey,
                                        value);
    aggregate.setAggregatorIndex(aggregatorIndex);
    return aggregate;
  }

  @Override
  public void aggregate(DimensionsEvent dest, DimensionsEvent src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    {
      byte[] destByte = destAggs.getFieldsByte();
      if(destByte != null) {
        byte[] srcByte = srcAggs.getFieldsByte();

        for(int index = 0;
            index < destByte.length;
            index++) {
          destByte[index] += srcByte[index];
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      if(destShort != null) {
        short[] srcShort = srcAggs.getFieldsShort();

        for(int index = 0;
            index < destShort.length;
            index++) {
          destShort[index] += srcShort[index];
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      if(destInteger != null) {
        int[] srcInteger = srcAggs.getFieldsInteger();

        for(int index = 0;
            index < destInteger.length;
            index++) {
          destInteger[index] += srcInteger[index];
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      if(destLong != null) {
        long[] srcLong = srcAggs.getFieldsLong();

        for(int index = 0;
            index < destLong.length;
            index++) {
          destLong[index] += srcLong[index];
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      if(destFloat != null) {
        float[] srcFloat = srcAggs.getFieldsFloat();

        for(int index = 0;
            index < destFloat.length;
            index++) {
          destFloat[index] += srcFloat[index];
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      if(destDouble != null) {
        double[] srcDouble = srcAggs.getFieldsDouble();

        for(int index = 0;
            index < destDouble.length;
            index++) {
          destDouble[index] += srcDouble[index];
        }
      }
    }
  }

  @Override
  public void aggregate(DimensionsEvent dest, EVENT src)
  {

  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorUtils.IDENTITY_NUMBER_TYPE_MAP.get(inputType);
  }
}
