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
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

/**
 * This {@link IncrementalAggregator} takes the max of the fields provided in the {@link InputEvent}.
 */
public class AggregatorMax extends AbstractIncrementalAggregator
{
  private static final long serialVersionUID = 201503120332L;

  public AggregatorMax()
  {
    //Do nothing
  }

  @Override
  public void aggregate(Aggregate dest, InputEvent src)
  {
    GPOMutable destAggs = dest.getAggregates();
    GPOMutable srcAggs = src.getAggregates();

    {
      byte[] destByte = destAggs.getFieldsByte();
      byte[] srcByte = srcAggs.getFieldsByte();
      int[] srcIndices = this.indexSubsetAggregates.fieldsByteIndexSubset;
      if(destByte != null) {
        for(int index = 0;
            index < destByte.length;
            index++) {
          byte tempByte = srcByte[srcIndices[index]];
          if(destByte[index] > tempByte) {
            destByte[index] = tempByte;
          }
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      short[] srcShort = srcAggs.getFieldsShort();
      int[] srcIndices = this.indexSubsetAggregates.fieldsShortIndexSubset;
      if(destShort != null) {
        for(int index = 0;
            index < destShort.length;
            index++) {
          short tempShort = srcShort[srcIndices[index]];
          if(destShort[index] > tempShort) {
            destShort[index] = tempShort;
          }
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      int[] srcInteger = srcAggs.getFieldsInteger();
      int[] srcIndices = this.indexSubsetAggregates.fieldsIntegerIndexSubset;
      if(destInteger != null) {
        for(int index = 0;
            index < destInteger.length;
            index++) {
          int tempInt = srcInteger[srcIndices[index]];
          if(destInteger[index] > tempInt) {
            destInteger[index] = tempInt;
          }
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      long[] srcLong = srcAggs.getFieldsLong();
      int[] srcIndices = this.indexSubsetAggregates.fieldsLongIndexSubset;
      if(destLong != null) {
        for(int index = 0;
            index < destLong.length;
            index++) {
          long tempLong = srcLong[srcIndices[index]];
          if(destLong[index] > tempLong) {
            destLong[index] = tempLong;
          }
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      float[] srcFloat = destAggs.getFieldsFloat();
      int[] srcIndices = this.indexSubsetAggregates.fieldsFloatIndexSubset;
      if(destFloat != null) {
        for(int index = 0;
            index < destFloat.length;
            index++) {
          float tempFloat = srcFloat[srcIndices[index]];
          if(destFloat[index] > tempFloat) {
            destFloat[index] = tempFloat;
          }
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      double[] srcDouble = destAggs.getFieldsDouble();
      int[] srcIndices = this.indexSubsetAggregates.fieldsDoubleIndexSubset;
      if(destDouble != null) {
        for(int index = 0;
            index < destDouble.length;
            index++) {
          double tempDouble = srcDouble[srcIndices[index]];
          if(destDouble[index] > tempDouble) {
            destDouble[index] = tempDouble;
          }
        }
      }
    }
  }

  @Override
  public void aggregate(Aggregate dest, Aggregate src)
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
          if(destByte[index] < srcByte[index]) {
            destByte[index] = srcByte[index];
          }
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
          if(destShort[index] < srcShort[index]) {
            destShort[index] = srcShort[index];
          }
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
          if(destInteger[index] < srcInteger[index]) {
            destInteger[index] = srcInteger[index];
          }
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
          if(destLong[index] < srcLong[index]) {
            destLong[index] = srcLong[index];
          }
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
          if(destFloat[index] < srcFloat[index]) {
            destFloat[index] = srcFloat[index];
          }
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
          if(destDouble[index] < srcDouble[index]) {
            destDouble[index] = srcDouble[index];
          }
        }
      }
    }
  }

  @Override
  public Type getOutputType(Type inputType)
  {
    return AggregatorUtils.IDENTITY_NUMBER_TYPE_MAP.get(inputType);
  }
}
