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
import com.datatorrent.lib.appdata.gpo.GPOGetters;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;

import java.util.List;
import java.util.Map;

public class GenericDimensionsComputationSingleSchemaPOJO extends GenericDimensionsComputationSingleSchema<Object>
{
  private transient GPOGetters gpoGettersKey;
  private transient GPOGetters gpoGettersValue;

  private boolean needToCreateGetters = true;
  private Map<String, String> keyToExpression;
  private Map<String, String> aggregateToExpression;

  public GenericDimensionsComputationSingleSchemaPOJO()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    inputEvent = new InputEvent(
                 new EventKey(0,
                         0,
                         0,
                         new GPOMutable(this.configurationSchema.getKeyDescriptorWithTime())),
                 new GPOMutable(this.configurationSchema.getInputValuesDescriptor()));

  }

  @Override
  public void convert(InputEvent inputEvent, Object event)
  {
    if(needToCreateGetters) {
      needToCreateGetters = false;
      gpoGettersKey = createGetters(this.configurationSchema.getKeyDescriptorWithTime(),
                                    keyToExpression,
                                    event);
      gpoGettersValue = createGetters(this.configurationSchema.getInputValuesDescriptor(),
                                      aggregateToExpression,
                                      event);
    }

    GPOUtils.copyPOJOToGPO(inputEvent.getKeys(), gpoGettersKey, event);
    GPOUtils.copyPOJOToGPO(inputEvent.getAggregates(), gpoGettersValue, event);
  }

  private GPOGetters createGetters(FieldsDescriptor fieldsDescriptor,
                                   Map<String, String> valueToExpression,
                                   Object event)
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
                                                                   event.getClass());

          break;
        }
        case STRING: {
          gpoGetters.gettersString = GPOUtils.createGetterString(fields,
                                                                 valueToExpression,
                                                                 event.getClass());

          break;
        }
        case CHAR: {
          gpoGetters.gettersChar = GPOUtils.createGetterChar(fields,
                                                             valueToExpression,
                                                             event.getClass());

          break;
        }
        case DOUBLE: {
          gpoGetters.gettersDouble = GPOUtils.createGetterDouble(fields,
                                                                 valueToExpression,
                                                                 event.getClass());

          break;
        }
        case FLOAT: {
          gpoGetters.gettersFloat = GPOUtils.createGetterFloat(fields,
                                                               valueToExpression,
                                                               event.getClass());
          break;
        }
        case LONG: {
          gpoGetters.gettersLong = GPOUtils.createGetterLong(fields,
                                                             valueToExpression,
                                                             event.getClass());

          break;
        }
        case INTEGER: {
          gpoGetters.gettersInteger = GPOUtils.createGetterInt(fields,
                                                               valueToExpression,
                                                               event.getClass());

          break;
        }
        case SHORT: {
          gpoGetters.gettersShort = GPOUtils.createGetterShort(fields,
                                                               valueToExpression,
                                                               event.getClass());

          break;
        }
        case BYTE: {
          gpoGetters.gettersByte = GPOUtils.createGetterByte(fields,
                                                             valueToExpression,
                                                             event.getClass());

          break;
        }
        case OBJECT: {
          gpoGetters.gettersObject = GPOUtils.createGetterObject(fields,
                                                                 valueToExpression,
                                                                 event.getClass());

          break;
        }
        default: {
          throw new IllegalArgumentException("The type " + inputType + " is not supported.");
        }
      }
    }

    return gpoGetters;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  /**
   * @return the keyToExpression
   */
  public Map<String, String> getKeyToExpression()
  {
    return keyToExpression;
  }

  /**
   * @param keyToExpression the keyToExpression to set
   */
  public void setKeyToExpression(Map<String, String> keyToExpression)
  {
    this.keyToExpression = keyToExpression;
  }

  /**
   * @return the aggregateToExpression
   */
  public Map<String, String> getAggregateToExpression()
  {
    return aggregateToExpression;
  }

  /**
   * @param aggregateToExpression the aggregateToExpression to set
   */
  public void setAggregateToExpression(Map<String, String> aggregateToExpression)
  {
    this.aggregateToExpression = aggregateToExpression;
  }
}
