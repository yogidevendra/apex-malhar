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

package org.apache.apex.malhar.lib.fs.s3;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;

@org.apache.hadoop.classification.InterfaceStability.Evolving
public class S3StringOutputModule implements Module
{
  public final transient ProxyInputPort<String> input = new ProxyInputPort<String>();

  /**
   * AWS access key
   */
  @NotNull
  private String accessKey;
  /**
   * AWS secret access key
   */
  @NotNull
  private String secretAccessKey;

  /**
   * S3 End point
   */
  private String endPoint;
  /**
   * Name of the bucket in which to upload the files
   */
  @NotNull
  private String bucketName;

  /**
   * Path of the output directory. Relative path of the files copied will be
   * maintained w.r.t. source directory and output directory
   */
  @NotNull
  private String outputDirectoryPath;

  /**
   * Max number of idle windows for which no new data is added to current part
   * file. Part file will be finalized after these many idle windows after last
   * new data.
   */
  private long maxIdleWindows = 100;

  /**
   * The maximum length in bytes of a rolling file. The default value of this is Long.MAX_VALUE
   */
  @Min(1)
  protected Long maxLength = 1 * 1000 * 1000L;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    S3CompactionOperator<String> s3compaction = dag.addOperator("S3Compaction", new S3CompactionOperator());
    s3compaction.setConverter(new GenericFileOutputOperator.StringToBytesConverter());
    s3compaction.setMaxIdleWindows(maxIdleWindows);
    s3compaction.setMaxLength(maxLength);

    S3Reconciler s3Reconciler = dag.addOperator("S3Reconciler", new S3Reconciler());
    s3Reconciler.setAccessKey(accessKey);
    s3Reconciler.setSecretKey(secretAccessKey);
    s3Reconciler.setBucketName(bucketName);
    s3Reconciler.setDirectoryName(outputDirectoryPath);

    if (endPoint != null) {
      s3Reconciler.setEndPoint(endPoint);
    }
    input.set(s3compaction.input);
    dag.addStream("write-to-s3", s3compaction.output, s3Reconciler.input);
  }

  /**
   * Get the AWS access key
   *
   * @return AWS access key
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set the AWS access key
   *
   * @param accessKey
   *          access key
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  /**
   * Return the AWS secret access key
   *
   * @return AWS secret access key
   */
  public String getSecretAccessKey()
  {
    return secretAccessKey;
  }

  /**
   * Set the AWS secret access key
   *
   * @param secretAccessKey
   *          AWS secret access key
   */
  public void setSecretAccessKey(String secretAccessKey)
  {
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * Get the name of the bucket in which to upload the files
   *
   * @return bucket name
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set the name of the bucket in which to upload the files
   *
   * @param bucketName
   *          name of the bucket
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  /**
   * Return the S3 End point
   *
   * @return S3 End point
   */
  public String getEndPoint()
  {
    return endPoint;
  }

  /**
   * Set the S3 End point
   *
   * @param endPoint
   *          S3 end point
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * Get the path of the output directory.
   *
   * @return path of output directory
   */
  public String getOutputDirectoryPath()
  {
    return outputDirectoryPath;
  }

  /**
   * Set the path of the output directory.
   *
   * @param outputDirectoryPath
   *          path of output directory
   */
  public void setOutputDirectoryPath(String outputDirectoryPath)
  {
    this.outputDirectoryPath = outputDirectoryPath;
  }

  /**
   * @return max number of idle windows for rollover
   */
  public long getMaxIdleWindows()
  {
    return maxIdleWindows;
  }

  /**
   * @param maxIdleWindows
   *          max number of idle windows for rollover
   */
  public void setMaxIdleWindows(long maxIdleWindows)
  {
    this.maxIdleWindows = maxIdleWindows;
  }

  public Long getMaxLength()
  {
    return maxLength;
  }

  public void setMaxLength(Long maxLength)
  {
    this.maxLength = maxLength;
  }

}
