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

package com.datatorrent.lib.io.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * This class is responsible for writing tuples to HDFS. All tuples are written
 * to the same file. Rolling over to the next file based on file size is
 * supported.
 *
 * @param <T>
 */

class HDFSOutputOperator extends AbstractFileOutputOperator<byte[]>
{

  /**
   * Name of the file to write the output. Directory for the output file should
   * be specified in the filePath
   */
  @NotNull
  private String fileName;

  /**
   * currentPartName is of the form fileName_physicalPartionId.partNumber
   */
  private String currentPartNameformat = "%s_%d";

  /**
   * currentPartName is of the form fileName_physicalPartionId.partNumber
   */
  private transient String currentPartName;

  /**
   * Physical partition id for the current partition.
   */
  protected transient int physicalPartitionId;

  /**
   * Flag to mark if new data in current application window
   */
  private transient boolean isNewDataInCurrentWindow;

  /**
   * Separator between the tuples
   */
  private String tupleSeparator;

  /**
   * byte[] representation of tupleSeparator
   */
  private transient byte[] tupleSeparatorBytes;

  /**
   * Operator specific metric bytes per second
   */
  @AutoMetric
  private long bytesPerSec;

  /**
   * No. of bytes received in current application window
   */
  private long byteCount;

  /**
   * No. of tuples present in current output file
   */
  private long currentPartTupleCount;

  /**
   * Max. number of tuples allowed per part. Part file will be finalized after
   * these many tuples
   */
  private long maxTupleCount = Long.MAX_VALUE;

  /**
   * No. of windows passed from beginning of current file
   */
  private long currentPartElapsedWindows;

  /**
   * Max number of elapsed windows for which current part file is valid. Part
   * file will be finalized after these many windows elasped since the beginning
   * of current part file.
   */
  private long maxElapsedWindows = Long.MAX_VALUE;

  /**
   * No. of windows since last new data received
   */
  private long currentPartIdleWindows;

  /**
   * Max number of idle windows for which no new data is added to current part
   * file. Part file will be finalized after these many idle windows after last
   * new data.
   */
  private long maxIdleWindows = Long.MAX_VALUE;

  /**
   * Size of one application window in second
   */
  private transient double windowTimeSec;

  protected StreamCodec<String> stringStreamCodec;

  private static final long DEFAULT_STREAM_EXPIRY_ACCESS_MILL = 60 * 60 * 1000L; //1 hour
  private static final int DEFAULT_ROTATION_WINDOWS = 2 * 60 * 10; //10 min  

  public HDFSOutputOperator()
  {
    setTupleSeparator(System.getProperty("line.separator"));
    setExpireStreamAfterAccessMillis(DEFAULT_STREAM_EXPIRY_ACCESS_MILL);
    setRotationWindows(DEFAULT_ROTATION_WINDOWS);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    physicalPartitionId = context.getId();
    currentPartName = String.format(currentPartNameformat, fileName, physicalPartitionId);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected String getFileName(byte[] tuple)
  {
    return currentPartName;
  }

  /**
   * This input port receives incoming tuples.
   */
  public final transient DefaultInputPort<String> stringInput = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      processTuple(tuple.getBytes());
    }

    @Override
    public StreamCodec<String> getStreamCodec()
    {
      if (HDFSOutputOperator.this.stringStreamCodec == null) {
        return super.getStreamCodec();
      } else {
        return stringStreamCodec;
      }
    }
  };

  /**
   * {@inheritDoc}
   * 
   * @return byte[] representation of the given tuple. if input tuple is of type
   *         byte[] then it is returned as it is. for any other type toString()
   *         representation is used to generate byte[].
   */
  @Override
  protected byte[] getBytesForTuple(byte[] tuple)
  {
    ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();

    try {
      bytesOutStream.write(tuple);
      bytesOutStream.write(tupleSeparatorBytes);
      byteCount += bytesOutStream.size();
      return bytesOutStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        bytesOutStream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    LOG.debug("beginWindow : {}" , windowId);
    super.beginWindow(windowId);
    bytesPerSec = 0;
    byteCount = 0;

    isNewDataInCurrentWindow = false;
  }

  @Override
  protected void processTuple(byte[] tuple)
  {
    LOG.debug("currentPartName : {}", currentPartName);
    super.processTuple(tuple);
    isNewDataInCurrentWindow = true;

    if (++currentPartTupleCount == maxTupleCount) {
      requestFinalize(currentPartName);
    }
  }

  @Override
  public void endWindow()
  {
    LOG.debug("endWindow");
    super.endWindow();
    bytesPerSec = (long)(byteCount / windowTimeSec);

    if (!isNewDataInCurrentWindow) {
      ++currentPartIdleWindows;
    } else {
      currentPartIdleWindows = 0;
    }

    ++currentPartElapsedWindows;

    //request for finalization if there is no new data in current application window.
    //This is done automatically if the file is rotated periodically or has a size threshold.
    if (checkEndWindowFinalization()) {
      rotateCall(currentPartName);
    }
  }

  /**
   * 
   */
  private boolean checkEndWindowFinalization()
  {
    if ((currentPartIdleWindows == maxIdleWindows || currentPartElapsedWindows == maxElapsedWindows)
        && !endOffsets.isEmpty()) {
      return true;
    }
    return false;
  }


  protected void rotateCall(String lastFile)
  {
    try {
      this.rotate(lastFile);
      currentPartName = String.format(currentPartNameformat, fileName, physicalPartitionId);
      currentPartIdleWindows = 0;
      currentPartElapsedWindows = 0;
      currentPartTupleCount = 0;
    }
    catch (IOException ex) {
      LOG.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
    catch (ExecutionException ex) {
      LOG.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
  }
  
  /**
   * @return File name for writing output. All tuples are written to the same
   *         file.
   * 
   */
  public String getFileName()
  {
    return fileName;
  }

  /**
   * @param fileName
   *          File name for writing output. All tuples are written to the same
   *          file.
   */
  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }

  public String getCurrentPartNameformat()
  {
    return currentPartNameformat;
  }

  public void setCurrentPartNameformat(String currentPartNameformat)
  {
    this.currentPartNameformat = currentPartNameformat;
  }

  public String getCurrentPartName()
  {
    return currentPartName;
  }

  public void setCurrentPartName(String currentPartName)
  {
    this.currentPartName = currentPartName;
  }

  /**
   * @return Separator between the tuples
   */
  public String getTupleSeparator()
  {
    return tupleSeparator;
  }

  /**
   * @param separator
   *          Separator between the tuples
   */
  public void setTupleSeparator(String separator)
  {
    this.tupleSeparator = separator;
    this.tupleSeparatorBytes = separator.getBytes();
  }

  public long getMaxTupleCount()
  {
    return maxTupleCount;
  }

  public void setMaxTupleCount(long maxTupleCount)
  {
    this.maxTupleCount = maxTupleCount;
  }

  public long getMaxElapsedWindows()
  {
    return maxElapsedWindows;
  }

  public void setMaxElapsedWindows(long maxElapsedWindows)
  {
    this.maxElapsedWindows = maxElapsedWindows;
  }
  
  public long getMaxIdleWindows()
  {
    return maxIdleWindows;
  }
  
  public void setMaxIdleWindows(long maxIdleWindows)
  {
    this.maxIdleWindows = maxIdleWindows;
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(HDFSOutputOperator.class);
}
