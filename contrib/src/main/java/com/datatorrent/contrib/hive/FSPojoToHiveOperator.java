/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.hive;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.converters.AbstractArrayConverter;
import org.apache.commons.beanutils.converters.AbstractConverter;
import org.apache.commons.beanutils.converters.ArrayConverter;

import com.google.common.collect.Lists;

import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterShort;

/**
 * An Implementation of AbstractFSRollingOutputOperator which takes any POJO as input, serializes the POJO as Hive delimiter separated values
 * which are written in text files to hdfs, and are inserted into hive on committed window callback.This operator can handle outputting to multiple files when the output file depends on the tuple.
 *
 * @displayName: FS To Hive Operator
 * @category Output
 * @tags fs, hive, database
 * @since 3.0.0
 */
public class FSPojoToHiveOperator<T> extends AbstractFSRollingOutputOperator<T>
{
  private static final long serialVersionUID = 1L;
  
  private String[] hivePartitionColumns;
  private String[] hiveColumns;
  private FIELD_TYPE[] hiveColumnDataTypes;
  private FIELD_TYPE[] hivePartitionColumnDataTypes;
  private transient ArrayList<Object> getters = Lists.newArrayList();
  private String[] expressionsForHiveColumns;
  private String[] expressionsForHivePartitionColumns;

  public String[] getExpressionsForHivePartitionColumns()
  {
    return expressionsForHivePartitionColumns;
  }

  public void setExpressionsForHivePartitionColumns(String[] expressionsForHivePartitionColumns)
  {
    this.expressionsForHivePartitionColumns = expressionsForHivePartitionColumns;
  }

  /*
   * A list of Java expressions in which each expression yields the specific table column value and partition column value in hive table from the input POJO.
   */
  public String[] getExpressionsForHiveColumns()
  {
    return expressionsForHiveColumns;
  }

  public void setExpressionsForHiveColumns(String[] expressions)
  {
    this.expressionsForHiveColumns = expressions;
  }

  @SuppressWarnings("unchecked")
  private void getValue(Object tuple, int index, FIELD_TYPE type, StringBuilder value)
  {
    switch (type) {
      case CHARACTER:
        value.append(((GetterChar<Object>)getters.get(index)).get(tuple));
        break;
      case STRING:
        value.append(((Getter<Object, String>)getters.get(index)).get(tuple));
        break;
      case BOOLEAN:
        value.append(((GetterBoolean<Object>)getters.get(index)).get(tuple));
        break;
      case SHORT:
        value.append(((GetterShort<Object>)getters.get(index)).get(tuple));
        break;
      case INTEGER:
        value.append(((GetterInt<Object>)getters.get(index)).get(tuple));
        break;
      case LONG:
        value.append(((GetterLong<Object>)getters.get(index)).get(tuple));
        break;
      case FLOAT:
        value.append(((GetterFloat<Object>)getters.get(index)).get(tuple));
        break;
      case DOUBLE:
        value.append(((GetterDouble<Object>)getters.get(index)).get(tuple));
        break;
      case DATE:
        value.append(((Getter<Object, Date>)getters.get(index)).get(tuple));
        break;
      case TIMESTAMP:
        value.append(((Getter<Object, Timestamp>)getters.get(index)).get(tuple));
        break;
      case OTHER:
        value.append(((Getter<Object, Object>)getters.get(index)).get(tuple));
        break;
      default:
        throw new RuntimeException("unsupported data type " + type);
    }
  }

  public enum FIELD_TYPE
  {
    BOOLEAN, DOUBLE, INTEGER, FLOAT, LONG, SHORT, CHARACTER, STRING, DATE, TIMESTAMP, OTHER
  }
  
  
  static{
    //Code for enabling BeanUtils to accept comma separated string to initialize FIELD_TYPE[]
    class FieldTypeConvertor extends AbstractConverter{

      @Override
      protected Object convertToType(Class type, Object value) throws Throwable
      {
        if(value instanceof String){
          return FIELD_TYPE.valueOf((String)value);
        }
        else{
          throw new IllegalArgumentException("FIELD_TYPE should be specified as String");
        }
      }

      @Override
      protected Class getDefaultType()
      {
        return FIELD_TYPE.class;
      }
    }
    
    class FieldTypeArrayConvertor extends ArrayConverter{

      public FieldTypeArrayConvertor()
      {
        super(FIELD_TYPE[].class, new FieldTypeConvertor());
      }
    }
    
    ConvertUtils.register(new FieldTypeArrayConvertor(), FIELD_TYPE[].class);
  }

  /*
   * Columns in Hive table.
   */
  public String[] getHiveColumns()
  {
    return hiveColumns;
  }

  public void setHiveColumns(String[] hiveColumns)
  {
    this.hiveColumns = hiveColumns;
  }

  /*
   * Partition Columns in Hive table.Example: dt for date,ts for timestamp
   */
  public String[] getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(String[] hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  /*
   * Data Types of Hive table data columns.
   * Example: If the Hive table has two columns of data type int and float,
   * then hiveColumnsDataTypes = {INTEGER,FLOAT}.
   * Particular Data Type can be chosen from the List of data types provided.
   */
  public FIELD_TYPE[] getHiveColumnDataTypes()
  {
    return hiveColumnDataTypes;
  }

  public void setHiveColumnDataTypes(FIELD_TYPE[] hiveColumnDataTypes)
  {
    this.hiveColumnDataTypes = hiveColumnDataTypes;
  }
  
  /*
   * Data Types of Hive table Partition Columns.
   * Example: If the Hive table has two columns of data type int and float and is partitioned by date of type String,
   * then hivePartitionColumnDataTypes = {STRING}.
   * Particular Data Type can be chosen from the List of data types provided.
   */
  public FIELD_TYPE[] getHivePartitionColumnDataTypes()
  {
    return hivePartitionColumnDataTypes;
  }

  public void setHivePartitionColumnDataTypes(FIELD_TYPE[] hivePartitionColumnDataTypes)
  {
    this.hivePartitionColumnDataTypes = hivePartitionColumnDataTypes;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ArrayList<String> getHivePartition(T tuple)
  {
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
    
    int sizeOfColumns = hiveColumns.length;
    int sizeOfPartitionColumns = hivePartitionColumns.length;
    //int size = sizeOfColumns + sizeOfPartitionColumns;
    ArrayList<String> hivePartitionColumnValues = new ArrayList<String>();
    String partitionColumnValue;
    for (int i = 0; i < sizeOfPartitionColumns; i++) {
      FIELD_TYPE type = hivePartitionColumnDataTypes[i];
      StringBuilder result = new StringBuilder();
      getValue(tuple, sizeOfColumns + i, type, result);
      partitionColumnValue = result.toString();
      //partitionColumnValue = (String)getters.get(i).get(tuple);
      hivePartitionColumnValues.add(partitionColumnValue);
    }
    return hivePartitionColumnValues;
  }

  @Override
  public void processTuple(T tuple)
  {
    if (getters.isEmpty()) {
      processFirstTuple(tuple);
    }
    super.processTuple(tuple);
  }

  @SuppressWarnings("unchecked")
  public void processFirstTuple(T tuple)
  {
    Class<?> fqcn = tuple.getClass();
    createGetters(fqcn, hiveColumns.length, expressionsForHiveColumns,hiveColumnDataTypes);
    createGetters(fqcn, hivePartitionColumns.length, expressionsForHivePartitionColumns,hivePartitionColumnDataTypes);
  }

  protected void createGetters(Class<?> fqcn, int size, String[] expressions,FIELD_TYPE[] columnDataTypes)
  {
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = columnDataTypes[i];
      final Object getter;
      final String getterExpression = expressions[i];
      switch (type) {
        case CHARACTER:
          getter = PojoUtils.createGetterChar(fqcn, getterExpression);
          break;
        case STRING:
          getter = PojoUtils.createGetter(fqcn, getterExpression, String.class);
          break;
        case BOOLEAN:
          getter = PojoUtils.createGetterBoolean(fqcn, getterExpression);
          break;
        case SHORT:
          getter = PojoUtils.createGetterShort(fqcn, getterExpression);
          break;
        case INTEGER:
          getter = PojoUtils.createGetterInt(fqcn, getterExpression);
          break;
        case LONG:
          getter = PojoUtils.createGetterLong(fqcn, getterExpression);
          break;
        case FLOAT:
          getter = PojoUtils.createGetterFloat(fqcn, getterExpression);
          break;
        case DOUBLE:
          getter = PojoUtils.createGetterDouble(fqcn, getterExpression);
          break;
        case DATE:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Date.class);
          break;
        case TIMESTAMP:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Timestamp.class);
          break;
        default:
          getter = PojoUtils.createGetter(fqcn, getterExpression, Object.class);
      }

      getters.add(getter);

    }

  }

  @Override
  @SuppressWarnings("unchecked")
  protected byte[] getBytesForTuple(T tuple)
  {
    int size = hiveColumns.length;
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < size; i++) {
      FIELD_TYPE type = hiveColumnDataTypes[i];
      getValue(tuple, i, type, result);
      result.append("\t");
    }
    result.append("\n");
    return (result.toString()).getBytes();
  }

}
