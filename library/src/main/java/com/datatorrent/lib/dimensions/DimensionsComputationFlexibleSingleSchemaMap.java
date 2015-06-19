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

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.Aggregate.Aggregate;
import com.datatorrent.lib.dimensions.Aggregate.EventKey;
import com.datatorrent.lib.dimensions.Aggregate.InputEvent;

import java.util.Map;

/**
 * This is a generic dimensions computation operator, which operates on input objects that are java
 * {@link Map}s. The keys of the maps are strings which represent the names of fields, and the values
 * are objects which represent the values of those fields.
 */
public class DimensionsComputationFlexibleSingleSchemaMap extends AbstractDimensionsComputationFlexibleSingleSchema<Map<String, Object>>
{
  /**
   * This is a map which defines aliases for field names in input maps. The key of this map is the name
   * of a field in the {@link DimensionalSchema} defined for this operator. The corresponding value in
   * this map is the name of the field in the input map. If there is no entry in this map for a field defined
   * in the {@link DimensionalSchema} then it is assumed that the name of the field is the same for input maps.
   */
  private Map<String, String> fieldToMapField;

  /**
   * Create a dimensions computation operator.
   */
  public DimensionsComputationFlexibleSingleSchemaMap()
  {
    this.unifier = new DimensionsComputationUnifierImpl<InputEvent, Aggregate>();
  }

  @Override
  public InputEvent convertInput(Map<String, Object> input, DimensionsConversionContext conversionContext)
  {
    FieldsDescriptor keyFieldsDescriptor = conversionContext.keyFieldsDescriptor;
    FieldsDescriptor valueFieldsDescriptor = conversionContext.aggregateDescriptor;

    GPOMutable keyMutable = new GPOMutable(keyFieldsDescriptor);

    for(int index = 0;
        index < keyFieldsDescriptor.getFieldList().size();
        index++) {
      String field = keyFieldsDescriptor.getFieldList().get(index);

      if(field.equals(DimensionsDescriptor.DIMENSION_TIME)) {
        long time = (Long) input.get(DimensionsDescriptor.DIMENSION_TIME);
        keyMutable.setField(DimensionsDescriptor.DIMENSION_TIME,
                            conversionContext.dd.getTimeBucket().roundDown(time));
      }
      else if(field.equals(DimensionsDescriptor.DIMENSION_TIME_BUCKET)) {
        keyMutable.setField(DimensionsDescriptor.DIMENSION_TIME_BUCKET,
                            conversionContext.dd.getTimeBucket().ordinal());
      }
      else {
        keyMutable.setField(field, input.get(getMapField(field)));
      }
    }

    GPOMutable valueMutable = new GPOMutable(valueFieldsDescriptor);

    for(int index = 0;
        index < valueFieldsDescriptor.getFieldList().size();
        index++) {
      String field = valueFieldsDescriptor.getFieldList().get(index);
      valueMutable.setField(field, input.get(getMapField(field)));
    }

    return new InputEvent(new EventKey(conversionContext.schemaID,
                                       conversionContext.dimensionDescriptorID,
                                       conversionContext.aggregatorID,
                                       keyMutable),
                          valueMutable);
  }

  /**
   * This is a helper method which gets the corresponding key string for a field in incoming {@link Map}s.
   * @param field The name of a field defined in the {@link DimensionalSchema}.
   * @return The key name of the given field in incoming input maps.
   */
  private String getMapField(String field)
  {
    if(fieldToMapField == null) {
      return field;
    }

    String mapField = fieldToMapField.get(field);

    if(mapField == null) {
      return field;
    }

    return mapField;
  }

  /**
   * Returns the mapping from {@link DimensionalSchema} field names to input map names.
   * @return The mapping from {@link DimensionalSchema} field names to input map names.
   */
  public Map<String, String> getFieldToMapField()
  {
    return fieldToMapField;
  }

  /**
   * Sets the mapping from {@link DimensionalSchema} field names to input map names.
   * @param fieldToMapField The mapping from {@link DimensionalSchema} field names to input map names.
   */
  public void setFieldToMapField(Map<String, String> fieldToMapField)
  {
    this.fieldToMapField = fieldToMapField;
  }
}
