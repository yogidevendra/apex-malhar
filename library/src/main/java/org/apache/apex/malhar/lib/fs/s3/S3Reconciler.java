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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.esotericsoftware.kryo.NotNull;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.fs.AbstractReconciler;

public class S3Reconciler extends AbstractReconciler<S3Reconciler.OutputMetaData, S3Reconciler.OutputMetaData>
{
  /**
   * Access key id for Amazon S3
   */
  @NotNull
  private String accessKey;
  
  /**
   * Secret key for Amazon S3
   */
  @NotNull
  private String secretKey;
  
  /**
   * Bucket name for data upload 
   */
  @NotNull
  private String bucketName;
  
  /**
   * S3 End point
   */
  private String endPoint;
  
  /**
   * Directory name under S3 bucket
   */
  @NotNull
  private String directoryName;
  
  /**
   * Client instance for connecting to Amazon S3
   */
  protected transient AmazonS3 s3client;
  
  /**
   * FileSystem instance for reading intermediate directory
   */
  protected transient FileSystem fs;
  
  protected transient String filePath;

  private static final String TMP_EXTENSION = ".tmp";
  
  @Override
  public void setup(Context.OperatorContext context)
  {
    s3client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + S3StringOutputModule.S3_INTERMEDIATE_DIR;
    try {
      fs = FileSystem.newInstance(new Path(filePath).toUri(), new Configuration());
    } catch (IOException e) {
      logger.error("Unable to create FileSystem: {}", e.getMessage());
    }
    super.setup(context);
  }

  /**
   * Enques the tuple for processing after committed callback
   */
  @Override
  protected void processTuple(S3Reconciler.OutputMetaData outputMetaData)
  {
    logger.debug("enque : {}", outputMetaData);
    enqueueForProcessing(outputMetaData);
  }

  /**
   * Uploads the file on Amazon S3 using putObject API from S3 client
   */
  @Override
  protected void processCommittedData(S3Reconciler.OutputMetaData outputMetaData)
  {
    try {
      FSDataInputStream fsinput = fs.open(new Path(outputMetaData.getPath()));
      ObjectMetadata omd = new ObjectMetadata();
      omd.setContentLength(outputMetaData.getSize());
      String keyName = directoryName + Path.SEPARATOR + outputMetaData.getFileName();
      s3client.putObject(new PutObjectRequest(bucketName, keyName, fsinput, omd));
      logger.debug("Uploading : {}", keyName);
    } catch (IOException e) {
      logger.error("Unable to create Stream: {}", e.getMessage());
    }
  }

  /**
   * Clears intermediate/temporary files if any
   */
  @Override
  public void endWindow()
  {
    logger.info("in endWindow()");
    while (doneTuples.peek() != null) {
      S3Reconciler.OutputMetaData metaData = doneTuples.poll();
      logger.info("found metaData = {}", metaData);
      committedTuples.remove(metaData);
      try {
        Path dest = new Path(metaData.getPath());
        //Deleting the intermediate files and when writing to tmp files
        // there can be vagrant tmp files which we have to clean
        FileStatus[] statuses = fs.listStatus(dest.getParent());
        logger.info("found statuses = {}", statuses);

        for (FileStatus status : statuses) {
          String statusName = status.getPath().getName();
          logger.info("statusName = {}", statusName);
          if (statusName.endsWith(TMP_EXTENSION) && statusName.startsWith(metaData.getFileName())) {
            //a tmp file has tmp extension always preceded by timestamp
            String actualFileName = statusName.substring(0,
                statusName.lastIndexOf('.', statusName.lastIndexOf('.') - 1));
            logger.info("actualFileName = {}", actualFileName);
            if (metaData.getFileName().equals(actualFileName)) {
              logger.info("deleting stray file {}", statusName);
              fs.delete(status.getPath(), true);
            }
          } else if (statusName.equals(metaData.getFileName())) {
            logger.info("deleting s3-compaction file {}", statusName);
            fs.delete(status.getPath(), true);
          }
        }
      } catch (IOException e) {
        logger.error("Unable to Delete a file: {}", metaData.getFileName());
      }
    }
  }

  /**
   * Get access key id
   * @return Access key id for Amazon S3
   */
  public String getAccessKey()
  {
    return accessKey;
  }

  /**
   * Set access key id
   * @param accessKey Access key id for Amazon S3
   */
  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  /**
   * Get secret key
   * @return Secret key for Amazon S3
   */
  public String getSecretKey()
  {
    return secretKey;
  }

  /**
   * Set secret key
   * @param secretKey Secret key for Amazon S3
   */
  public void setSecretKey(String secretKey)
  {
    this.secretKey = secretKey;
  }

  /**
   * Get bucket name
   * @return Bucket name for data upload
   */
  public String getBucketName()
  {
    return bucketName;
  }

  /**
   * Set bucket name
   * @param bucketName Bucket name for data upload
   */
  public void setBucketName(String bucketName)
  {
    this.bucketName = bucketName;
  }

  /**
   * Get directory name 
   * @return Directory name under S3 bucket
   */
  public String getDirectoryName()
  {
    return directoryName;
  }

  /**
   * Set directory name 
   * @param directoryName Directory name under S3 bucket
   */
  public void setDirectoryName(String directoryName)
  {
    this.directoryName = directoryName;
  }
  
  /**
   * Return the S3 End point
   * @return S3 End point
   */
  public String getEndPoint()
  {
    return endPoint;
  }

  /**
   * Set the S3 End point
   * @param endPoint S3 end point
   */
  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }

  /**
   * Set Amazon S3 client
   * @param s3client Client for Amazon S3 
   */
  void setS3client(AmazonS3 s3client)
  {
    this.s3client = s3client;
  }

  /**
   * Metadata for file upload to S3
   */
  public static class OutputMetaData
  {
    private String path;
    private String fileName;
    private long size;

    public OutputMetaData()
    {

    }

    public OutputMetaData(String path, String fileName, long size)
    {
      this.path = path;
      this.fileName = fileName;
      this.size = size;
    }

    public String getPath()
    {
      return path;
    }

    public void setPath(String path)
    {
      this.path = path;
    }

    public String getFileName()
    {
      return fileName;
    }

    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }

    public long getSize()
    {
      return size;
    }

    public void setSize(long size)
    {
      this.size = size;
    }
  }
  
  private static final Logger logger = LoggerFactory.getLogger(S3Reconciler.class);
  
}
