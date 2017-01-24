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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class S3StringOutputModuleTest
{

  
  private String accessKey = "******";
  private String secretKey = "******";
  public String bucketKey = "org.apache.apex.s3.test-bucket";
  private AmazonS3 client;

  private static class Application implements StreamingApplication
  {
    public void populateDAG(DAG dag, Configuration conf)
    {
      LineByLineFileInputOperator lineInputOperator = dag.addOperator("lineInput", LineByLineFileInputOperator.class);
      S3StringOutputModule s3StringOutputModule = dag.addModule("s3output", S3StringOutputModule.class);
      dag.addStream("data", lineInputOperator.output, s3StringOutputModule.input);
    }
  }

  private class TestMeta extends TestWatcher
  {
    String baseDirectory;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      baseDirectory = new File(
          "target" + Path.SEPARATOR + description.getClassName() + Path.SEPARATOR + description.getMethodName())
              .getPath();

      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < 1000; i++) {
        sb.append("Record ").append(i).append("|").append(RandomStringUtils.randomAlphanumeric(100)).append("|\n");
      }

      File file = new File(baseDirectory, "input.txt");
      try {
        FileUtils.writeStringToFile(file, sb.toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
    client.createBucket(bucketKey);
  }

  @After
  public void tearDown() throws IOException
  {
    //Get the list of objects
    ObjectListing objectListing = client.listObjects(bucketKey);
    for (Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext();) {
      S3ObjectSummary objectSummary = (S3ObjectSummary)iterator.next();
      LOG.info("Deleting an object: {}", objectSummary.getKey());
      //client.deleteObject(bucketKey, objectSummary.getKey());
    }
    //client.deleteBucket(bucketKey);
  }

  @Test
  public void testApplication() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.set("dt.operator.lineInput.prop.directory", testMeta.baseDirectory);
      conf.set("dt.operator.s3output.prop.accessKey", accessKey);
      conf.set("dt.operator.s3output.prop.secretAccessKey", secretKey);
      conf.set("dt.operator.s3output.prop.bucketName", bucketKey);
      conf.set("dt.operator.s3output.prop.outputDirectoryPath", "test");
      conf.set("dt.operator.s3output.prop.maxIdleWindows", "10");
      conf.set("dt.operator.s3output.prop.maxLength", "10000");

      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();

      lc.setHeartbeatMonitoringEnabled(false);

      //      ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
      //      {
      //        @Override
      //        public Boolean call() throws Exception
      //        {
      //          if (selectedfile.exists() && rejectedfile.exists()) {
      //            return true;
      //          }
      //          return false;
      //        }
      //      });
      //
      //      lc.run(40000);

      lc.runAsync();
      Thread.sleep(2 * 60 * 1000);
      lc.shutdown();

      LOG.debug("Bucket listing: {}",client.listObjects(bucketKey).getObjectSummaries());

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private static Logger LOG = LoggerFactory.getLogger(S3StringOutputModuleTest.class);
}
