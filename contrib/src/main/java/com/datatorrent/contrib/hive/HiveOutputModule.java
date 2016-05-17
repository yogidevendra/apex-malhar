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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.contrib.hive.FSPojoToHiveOperator.FIELD_TYPE;

/**
 * HiveOutputModule provides abstraction for the operators needed for writing tuples to hive.
 * This module will be expanded to FSPojoToHiveOperator and HiveOperator in physical plan. 
 */

public class HiveOutputModule implements Module
{
  
  /**
   * The path of the directory to where files are written.
   */
  @NotNull
  private String filePath;
  
  /**
   * Names of the columns in hive table (excluding partitioning columns). 
   */
  private String[] hiveColumns;
  
  /**
   * Data types of the columns in hive table (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   */
  private FIELD_TYPE[] hiveColumnDataTypes;
  
  /**
   * Expressions for the hive columns (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   */
  private String[] expressionsForHiveColumns;
  
  /**
   * Names of the columns on which hive data should be partitioned 
   */
  private String[] hivePartitionColumns;
  
  /**
   * Data types of the columns on which hive data should be partitioned.
   * This sequence should match to the fields in hivePartitionColumns 
   */
  private FIELD_TYPE[] hivePartitionColumnDataTypes;
  
  /**
   * Expressions for the hive partition columns.
   * This sequence should match to the fields in hivePartitionColumns
   */
  private String[] expressionsForHivePartitionColumns;
  
  /**
   * The maximum length in bytes of a rolling file. The default value of this is Long.MAX_VALUE
   */
  @Min(1)
  protected Long maxLength = Long.MAX_VALUE;
  
  /**
   * Input port for files metadata.
   */
  public final transient ProxyInputPort<Object> input = new ProxyInputPort<Object>();
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSPojoToHiveOperator<Object> fsRolling = dag.addOperator("fsRolling", new FSPojoToHiveOperator<Object>());
    HiveOperator hiveOperator = dag.addOperator("HiveOperator", new HiveOperator());
    
    input.set(fsRolling.input);
    dag.addStream("toHive", fsRolling.outputPort, hiveOperator.input);
    
    fsRolling.setHivePartitionColumns(hiveColumns);
    fsRolling.setHiveColumnDataTypes(hiveColumnDataTypes);
    fsRolling.setExpressionsForHiveColumns(expressionsForHiveColumns);
    
    fsRolling.setHivePartitionColumns(hivePartitionColumns);
    fsRolling.setHivePartitionColumnDataTypes(hivePartitionColumnDataTypes);
    fsRolling.setExpressionsForHivePartitionColumns(expressionsForHivePartitionColumns);
    
    fsRolling.setMaxLength(maxLength);
    
    hiveOperator.setHivePartitionColumns(hivePartitionColumns);
    
    
  }
  
  /**
   * Names of the columns in hive table (excluding partitioning columns). 
   * @return Hive column names
   */
  public String[] getHiveColumns()
  {
    return hiveColumns;
  }
  
  /**
   * Names of the columns in hive table (excluding partitioning columns). 
   * @param hiveColumns Hive column names
   */
  public void setHiveColumns(String[] hiveColumns)
  {
    this.hiveColumns = hiveColumns;
  }
  
  /**
   * Data types of the columns in hive table (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   * @return Hive column data types
   */
  public FIELD_TYPE[] getHiveColumnDataTypes()
  {
    return hiveColumnDataTypes;
  }
  
  /**
   * Data types of the columns in hive table (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes   * 
   * @param hiveColumnDataTypes Hive column data types
   */
  public void setHiveColumnDataTypes(FIELD_TYPE[] hiveColumnDataTypes)
  {
    this.hiveColumnDataTypes = hiveColumnDataTypes;
  }
  
  /**
   * Expressions for the hive columns (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   * @return
   */
  public String[] getExpressionsForHiveColumns()
  {
    return expressionsForHiveColumns;
  }
  
  /**
   * Expressions for the hive columns (excluding partitioning columns).
   * This sequence should match to the fields in hiveColumnDataTypes
   * @param expressionsForHiveColumns
   */
  public void setExpressionsForHiveColumns(String[] expressionsForHiveColumns)
  {
    this.expressionsForHiveColumns = expressionsForHiveColumns;
  }

  /**
   * Names of the columns on which hive data should be partitioned 
   * @return hive partition columns
   */
  public String[] getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  /**
   * Names of the columns on which hive data should be partitioned 
   * @param hivePartitionColumns Hive partition columns
   */
  public void setHivePartitionColumns(String[] hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  /**
   * Data types of the columns on which hive data should be partitioned.
   * This sequence should match to the fields in hivePartitionColumns
   * @return Hive partition column data types
   */
  public FIELD_TYPE[] getHivePartitionColumnDataTypes()
  {
    return hivePartitionColumnDataTypes;
  }
  
  /**
   * Data types of the columns on which hive data should be partitioned.
   * This sequence should match to the fields in hivePartitionColumns
   * @param hivePartitionColumnDataTypes Hive partition column data types
   */
  public void setHivePartitionColumnDataTypes(FIELD_TYPE[] hivePartitionColumnDataTypes)
  {
    this.hivePartitionColumnDataTypes = hivePartitionColumnDataTypes;
  }

  /**
   * Expressions for the hive partition columns.
   * This sequence should match to the fields in hivePartitionColumns
   * @return Expressions for hive partition columns
   */
  public String[] getExpressionsForHivePartitionColumns()
  {
    return expressionsForHivePartitionColumns;
  }
  
  /**
   * Expressions for the hive partition columns. 
   * This sequence should match to the fields in hivePartitionColumns 
   * @param expressionsForHivePartitionColumns Expressions for hive partition columns 
   */
  public void setExpressionsForHivePartitionColumns(String[] expressionsForHivePartitionColumns)
  {
    this.expressionsForHivePartitionColumns = expressionsForHivePartitionColumns;
  }
  
  /**
   * 
   * @return
   */
  public Long getMaxLength()
  {
    return maxLength;
  }
  
  /**
   * 
   * @param maxLength
   */
  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }
}
