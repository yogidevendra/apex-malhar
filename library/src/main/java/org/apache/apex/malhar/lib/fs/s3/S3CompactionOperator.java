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
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;

public class S3CompactionOperator<INPUT> extends GenericFileOutputOperator<INPUT>
{
  /**
   * Output port for emitting metadata for finalized files.  
   */
  public transient DefaultOutputPort<S3Reconciler.OutputMetaData> output = new DefaultOutputPort<S3Reconciler.OutputMetaData>();
  
  /**
   * Queue for holding finalized files for emitting on output port 
   */
  private Queue<S3Reconciler.OutputMetaData> emitQueue = new LinkedBlockingQueue<S3Reconciler.OutputMetaData>();

  public S3CompactionOperator()
  {
    filePath = "";
    outputFileName = "s3-compaction";
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    filePath = context.getValue(DAG.APPLICATION_PATH) + Path.SEPARATOR + S3StringOutputModule.S3_INTERMEDIATE_DIR;
    super.setup(context);
  }

  @Override
  protected void finalizeFile(String fileName) throws IOException
  {

    super.finalizeFile(fileName);
    
    //Add finalized files to the queue
    String srcPath = filePath + Path.SEPARATOR + fileName;
    long offset = fs.getFileStatus(new Path(srcPath)).getLen();
    S3Reconciler.OutputMetaData metaData = new S3Reconciler.OutputMetaData(srcPath, fileName, offset);
    emitQueue.add(metaData);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    //Emit finalized files from the queue
    while (!emitQueue.isEmpty()) {
      output.emit(emitQueue.poll());
    }
  }
}
