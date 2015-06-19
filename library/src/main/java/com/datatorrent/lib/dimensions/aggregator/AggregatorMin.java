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
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

/**
 * This {@link IncrementalAggregator} takes the min of the fields provided in the {@link InputEvent}.
 * @param <EVENT> This is the type of the input event.
 */
public class AggregatorMin<EVENT> extends AbstractIncrementalAggregator<EVENT>
{
  private static final long serialVersionUID = 20154301648L;

  public AggregatorMin()
  {
    //Do nothing
  }

  @Override
  public void aggregate(DimensionsEvent dest, EVENT src)
  {
    GPOMutable destAggs = dest.getAggregates();

    {
      byte[] destByte = destAggs.getFieldsByte();
      GetterByte<Object>[] gettersByte = this.getValueGetters().gettersByte;

      if(destByte != null) {
        for(int index = 0;
            index < destByte.length;
            index++) {
          byte tempByte = gettersByte[index].get(src);
          if(destByte[index] > tempByte) {
            destByte[index] = tempByte;
          }
        }
      }
    }

    {
      short[] destShort = destAggs.getFieldsShort();
      GetterShort<Object>[] gettersShort = this.getValueGetters().gettersShort;

      if(destShort != null) {
        for(int index = 0;
            index < destShort.length;
            index++) {
          short tempShort = gettersShort[index].get(src);
          if(destShort[index] > tempShort) {
            destShort[index] = tempShort;
          }
        }
      }
    }

    {
      int[] destInteger = destAggs.getFieldsInteger();
      GetterInt<Object>[] gettersInteger = this.getValueGetters().gettersInteger;

      if(destInteger != null) {
        for(int index = 0;
            index < destInteger.length;
            index++) {
          int tempInt = gettersInteger[index].get(src);
          if(destInteger[index] > tempInt) {
            destInteger[index] = tempInt;
          }
        }
      }
    }

    {
      long[] destLong = destAggs.getFieldsLong();
      GetterLong<Object>[] gettersLong = this.getValueGetters().gettersLong;

      if(destLong != null) {
        for(int index = 0;
            index < destLong.length;
            index++) {
          long tempLong = gettersLong[index].get(src);
          if(destLong[index] > tempLong) {
            destLong[index] = tempLong;
          }
        }
      }
    }

    {
      float[] destFloat = destAggs.getFieldsFloat();
      GetterFloat<Object>[] gettersFloat = this.getValueGetters().gettersFloat;

      if(destFloat != null) {
        for(int index = 0;
            index < destFloat.length;
            index++) {
          float tempFloat = gettersFloat[index].get(src);
          if(destFloat[index] > tempFloat) {
            destFloat[index] = tempFloat;
          }
        }
      }
    }

    {
      double[] destDouble = destAggs.getFieldsDouble();
      GetterDouble<Object>[] gettersDouble = this.getValueGetters().gettersDouble;

      if(destDouble != null) {
        for(int index = 0;
            index < destDouble.length;
            index++) {
          double tempDouble = gettersDouble[index].get(src);
          if(destDouble[index] > tempDouble) {
            destDouble[index] = tempDouble;
          }
        }
      }
    }
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
          if(destByte[index] > srcByte[index]) {
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
          if(destShort[index] > srcShort[index]) {
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
          if(destInteger[index] > srcInteger[index]) {
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
          if(destLong[index] > srcLong[index]) {
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
          if(destFloat[index] > srcFloat[index]) {
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
          if(destDouble[index] > srcDouble[index]) {
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
