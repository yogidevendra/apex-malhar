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

package com.datatorrent.lib.io.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Queues;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.block.HDFSBlockReader;
import com.datatorrent.lib.io.fs.AbstractReconciler;
import com.datatorrent.lib.io.output.StitchedFileMetaData.BlockNotFoundException;
import com.datatorrent.lib.io.output.StitchedFileMetaData.StitchBlock;

/**
 * This is generic File Stitcher which can be used to merge data from one or more
 * files into single stitched file. StitchedFileMetaData defines constituents of the
 * stitched file.
 * 
 * This class uses Reconciler to 
 */
public class FileStitcher<T extends StitchedFileMetaData> extends AbstractReconciler<T, T>
{
  /**
   * Filesystem on which application is running
   */
  protected transient FileSystem appFS;
  
  /**
   * Destination file system
   */
  protected transient FileSystem outputFS;

  /**
   * Path for destination directory
   */
  @NotNull
  protected String filePath;
  
  /**
   * Path for blocks directory
   */
  protected transient String blocksDir;

  protected static final String PART_FILE_EXTENTION = "._COPYING_";

  /**
   * Queue maintaining successful files
   */
  protected Queue<T> successfulFiles = Queues.newLinkedBlockingQueue();
  /**
   * Queue maintaining skipped files
   */
  protected Queue<T> skippedFiles = Queues.newLinkedBlockingQueue();
  /**
   * Queue maintaining failed files
   */
  protected Queue<T> failedFiles = Queues.newLinkedBlockingQueue();

  /**
   * Output port for emitting completed stitched files metadata
   */
  public final transient DefaultOutputPort<T> completedFilesMetaOutput = new DefaultOutputPort<T>();
  
  private boolean writeChecksum = true;
  transient protected Path tempOutFilePath;

  @Override
  public void setup(Context.OperatorContext context)
  {

    blocksDir = context.getValue(DAGContext.APPLICATION_PATH) + Path.SEPARATOR + BlockWriter.SUBDIR_BLOCKS;

    try {
      outputFS = getOutputFSInstance();
      outputFS.setWriteChecksum(writeChecksum);
    } catch (IOException ex) {
      throw new RuntimeException("Exception in getting output file system.", ex);
    }
    try {
      appFS = getAppFSInstance();
    } catch (IOException ex) {
      try {
        outputFS.close();
      } catch (IOException e) {
        throw new RuntimeException("Exception in closing output file system.", e);
      }
      throw new RuntimeException("Exception in getting application file system.", ex);
    }

    super.setup(context); // Calling it at the end as the reconciler thread uses resources allocated above.
  }

  /* 
   * Calls super.endWindow() and sets counters 
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    T tuple;
    int size = doneTuples.size();
    for (int i = 0; i < size; i++) {
      tuple = doneTuples.peek();
      // If a tuple is present in doneTuples, it has to be also present in successful/failed/skipped
      // as processCommittedData adds tuple in successful/failed/skipped
      // and then reconciler thread add that in doneTuples 
      if (successfulFiles.contains(tuple)) {
        successfulFiles.remove(tuple);
        LOG.debug("File copy successful: {}", tuple.getStitchedFileRelativePath());
      } else if (skippedFiles.contains(tuple)) {
        skippedFiles.remove(tuple);
        LOG.debug("File copy skipped: {}", tuple.getStitchedFileRelativePath());
      } else if (failedFiles.contains(tuple)) {
        failedFiles.remove(tuple);
        LOG.debug("File copy failed: {}", tuple.getStitchedFileRelativePath());
      } else {
        throw new RuntimeException(
            "Tuple present in doneTuples but not in successfulFiles: " + tuple.getStitchedFileRelativePath());
      }
      completedFilesMetaOutput.emit(tuple);
      committedTuples.remove(tuple);
      doneTuples.poll();
    }
  }

 /**
 * 
 * @return Application FileSystem instance
 * @throws IOException
 */
  protected FileSystem getAppFSInstance() throws IOException
  {
    return FileSystem.newInstance((new Path(blocksDir)).toUri(), new Configuration());
  }

  /**
   * 
   * @return Destination FileSystem instance
   * @throws IOException
   */
  protected FileSystem getOutputFSInstance() throws IOException
  {
    return FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
  }

  @Override
  public void teardown()
  {
    super.teardown();

    boolean gotException = false;
    try {
      if (appFS != null) {
        appFS.close();
        appFS = null;
      }
    } catch (IOException e) {
      gotException = true;
    }

    try {
      if (outputFS != null) {
        outputFS.close();
        outputFS = null;
      }
    } catch (IOException e) {
      gotException = true;
    }
    if (gotException) {
      throw new RuntimeException("Exception while closing file systems.");
    }
  }

  /**
   * Enques incoming data for for processing
   */
  @Override
  protected void processTuple(T fileMetadata)
  {
    enqueueForProcessing(fileMetadata);
  }

  /**
   * Stitches the output file when all blocks for that file are commited
   */
  @Override
  protected void processCommittedData(T queueInput)
  {
    try {
      mergeOutputFile(queueInput);
    } catch (IOException e) {
      throw new RuntimeException("Unable to merge file: " + queueInput.getStitchedFileRelativePath(), e);
    }
  }

  /**
   * Read data from block files and write to output file. Information about
   * which block files should be read is specified in outFileMetadata
   * 
   * @param outFileMetadata
   * @return
   * @throws IOException
   */

  protected void mergeOutputFile(T outFileMetadata) throws IOException
  {
    mergeBlocks(outFileMetadata);
    successfulFiles.add(outFileMetadata);
    LOG.debug("Completed processing file: {} ", outFileMetadata.getStitchedFileRelativePath());
  }

  protected void mergeBlocks(T outFileMetadata) throws IOException
  {
    //when writing to tmp files there can be vagrant tmp files which we have to clean
    final Path dst = new Path(filePath, outFileMetadata.getStitchedFileRelativePath());
    PathFilter tempFileFilter = new PathFilter()
    {
      @Override
      public boolean accept(Path path)
      {
        return path.getName().startsWith(dst.getName()) && path.getName().endsWith(PART_FILE_EXTENTION);
      }
    };
    if (outputFS.exists(dst.getParent())) {
      FileStatus[] statuses = outputFS.listStatus(dst.getParent(), tempFileFilter);
      for (FileStatus status : statuses) {
        String statusName = status.getPath().getName();
        LOG.debug("deleting vagrant file {}", statusName);
        outputFS.delete(status.getPath(), true);
      }
    }
    tempOutFilePath = new Path(filePath,
        outFileMetadata.getStitchedFileRelativePath() + '.' + System.currentTimeMillis() + PART_FILE_EXTENTION);
    try {
      writeTempOutputFile(outFileMetadata);
      moveToFinalFile(outFileMetadata);
    } catch (BlockNotFoundException e) {
      LOG.info("Block file {} not found. Assuming recovery mode for file {}. ", e.getBlockPath(),
          outFileMetadata.getStitchedFileRelativePath());
      //Remove temp output file
      outputFS.delete(tempOutFilePath, false);
    }
  }

  /**
   * Writing all Stitch blocks to temporary file
   * @param outFileMetadata
   * @return
   * @throws IOException
   * @throws BlockNotFoundException
   */
  protected OutputStream writeTempOutputFile(T outFileMetadata) throws IOException, BlockNotFoundException
  {
    OutputStream outputStream = getOutputStream(tempOutFilePath);
    try {
      for (StitchBlock outputBlock : outFileMetadata.getStitchBlocksList()) {
        outputBlock.writeTo(appFS, blocksDir, outputStream);
      }
    } finally {
      outputStream.close();
    }
    return outputStream;
  }

  protected OutputStream getOutputStream(Path partFilePath) throws IOException
  {
    return outputFS.create(partFilePath);
  }

  /**
   * Moving temp output file to final file
   * @param stitchedFileMetaData
   * @throws IOException
   */
  protected void moveToFinalFile(T stitchedFileMetaData) throws IOException
  {
    Path destination = new Path(filePath, stitchedFileMetaData.getStitchedFileRelativePath());
    moveToFinalFile(tempOutFilePath, destination);
  }

  /**
   * Moving temp output file to final file
   * @param tempOutFilePath Temporary output file
   * @param destination Destination directory path
   * @throws IOException
   */
  protected void moveToFinalFile(Path tempOutFilePath, Path destination) throws IOException
  {
    Path src = Path.getPathWithoutSchemeAndAuthority(tempOutFilePath);
    Path dst = Path.getPathWithoutSchemeAndAuthority(destination);

    boolean moveSuccessful = false;
    if (!outputFS.exists(dst.getParent())) {
      outputFS.mkdirs(dst.getParent());
    }
    if (outputFS.exists(dst)) {
      outputFS.delete(dst, false);
    }
    moveSuccessful = outputFS.rename(src, dst);

    if (moveSuccessful) {
      LOG.debug("File {} moved successfully to destination folder.", dst);
    } else {
      throw new RuntimeException("Unable to move file from " + src + " to " + dst);
    }
  }

  public String getBlocksDir()
  {
    return blocksDir;
  }

  public void setBlocksDir(String blocksDir)
  {
    this.blocksDir = blocksDir;
  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = HDFSBlockReader.convertSchemeToLowerCase(filePath);
  }

  public boolean isWriteChecksum()
  {
    return writeChecksum;
  }

  public void setWriteChecksum(boolean writeChecksum)
  {
    this.writeChecksum = writeChecksum;
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileStitcher.class);
}
