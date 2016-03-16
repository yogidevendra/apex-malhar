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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.output.StitchedFileMetaData.BlockNotFoundException;
import com.datatorrent.lib.io.output.Synchronizer.OutputFileMetadata;

/**
 * HDFS file merger extends file merger to optimize for HDFS file copy usecase.
 * This uses fast merge from HDFS if destination filesystem is same as
 * application filesystem.
 */
public class HDFSFileMerger extends FileMerger
{
  /**
   * Fast merge is possible if append is allowed for output file system.
   */
  private transient boolean fastMergeActive;
  /**
   * Default block size for output file system
   */
  private transient long defaultBlockSize;
  /**
   * Decision maker to enable fast merge based on blocks directory, application
   * directory, block size
   */
  private transient FastMergerDecisionMaker fastMergerDecisionMaker;

  /**
   * Initializations based on output file system configuration
   */
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    fastMergeActive = outputFS.getConf().getBoolean("dfs.support.append", true)
        && appFS.getUri().equals(outputFS.getUri());
    LOG.debug("appFS.getUri():{}", appFS.getUri());
    LOG.debug("outputFS.getUri():{}", outputFS.getUri());
    defaultBlockSize = outputFS.getDefaultBlockSize(new Path(filePath));
    fastMergerDecisionMaker = new FastMergerDecisionMaker(blocksDir, appFS, defaultBlockSize);
  }

  /**
   * Uses fast merge if possible. Else, fall back to serial merge.
   */
  @Override
  protected void mergeBlocks(OutputFileMetadata fileMetadata) throws IOException
  {

    try {
      LOG.debug("fastMergeActive: {}", fastMergeActive);
      if (fastMergeActive && fastMergerDecisionMaker.isFastMergePossible(fileMetadata)
          && fileMetadata.getNumberOfBlocks() > 0) {
        LOG.debug("Using fast merge on HDFS.");
        concatBlocks(fileMetadata);
        return;
      }
      LOG.debug("Falling back to slow merge on HDFS.");
      super.mergeBlocks(fileMetadata);

    } catch (BlockNotFoundException e) {
      if (recover(fileMetadata)) {
        LOG.debug("Recovery attempt successful.");
        successfulFiles.add(fileMetadata);
      } else {
        failedFiles.add(fileMetadata);
      }
    }
  }

  /**
   * Fast merge using HDFS block concat
   * 
   * @param fileMetadata
   * @throws IOException
   */
  private void concatBlocks(OutputFileMetadata fileMetadata) throws IOException
  {
    Path outputFilePath = new Path(filePath, fileMetadata.getRelativePath());

    int numBlocks = fileMetadata.getNumberOfBlocks();
    long[] blocksArray = fileMetadata.getBlockIds();

    Path firstBlock = new Path(blocksDir, Long.toString(blocksArray[0]));
    if (numBlocks > 1) {
      Path[] blockFiles = new Path[numBlocks - 1]; // Leave the first block

      for (int index = 1; index < numBlocks; index++) {
        blockFiles[index - 1] = new Path(blocksDir, Long.toString(blocksArray[index]));
      }

      outputFS.concat(firstBlock, blockFiles);
    }

    moveToFinalFile(firstBlock, outputFilePath);
  }

  /**
   * Attempt for recovery if block concat is successful but temp file is not
   * moved to final file
   * 
   * @param iFileMetadata
   * @return
   * @throws IOException
   */
  @VisibleForTesting
  protected boolean recover(OutputFileMetadata iFileMetadata) throws IOException
  {
    Path firstBlockPath = new Path(blocksDir + Path.SEPARATOR + iFileMetadata.getBlockIds()[0]);
    Path outputFilePath = new Path(filePath, iFileMetadata.getRelativePath());
    if (appFS.exists(firstBlockPath)) {
      FileStatus status = appFS.getFileStatus(firstBlockPath);
      if (status.getLen() == iFileMetadata.getFileLength()) {
        moveToFinalFile(firstBlockPath, outputFilePath);
        return true;
      }
      LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
      return false;
    }

    if (outputFS.exists(outputFilePath)) {
      LOG.debug("Output file already present at the destination, nothing to recover.");
      return true;
    }
    LOG.error("Unable to recover in FileMerger for file: {}", outputFilePath);
    return false;
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileMerger.class);

  /**
   * Utility class to decide fast merge possibility
   */
  public static class FastMergerDecisionMaker
  {

    private String blocksDir;
    private FileSystem appFS;
    private long defaultBlockSize;

    public FastMergerDecisionMaker(String blocksDir, FileSystem appFS, long defaultBlockSize)
    {
      this.blocksDir = blocksDir;
      this.appFS = appFS;
      this.defaultBlockSize = defaultBlockSize;
    }

    /**
     * Checks if fast merge is possible for given settings for blocks directory,
     * application file system, block size
     * 
     * @param fileMetadata
     * @return
     * @throws IOException
     * @throws BlockNotFoundException
     */
    public boolean isFastMergePossible(OutputFileMetadata fileMetadata) throws IOException, BlockNotFoundException
    {
      short replicationFactor = 0;
      boolean sameReplicationFactor = true;
      boolean multipleOfBlockSize = true;

      int numBlocks = fileMetadata.getNumberOfBlocks();
      LOG.debug("fileMetadata.getNumberOfBlocks(): {}", fileMetadata.getNumberOfBlocks());
      long[] blocksArray = fileMetadata.getBlockIds();
      LOG.debug("fileMetadata.getBlockIds().len: {}", fileMetadata.getBlockIds().length);

      for (int index = 0; index < numBlocks && (sameReplicationFactor && multipleOfBlockSize); index++) {
        Path blockFilePath = new Path(blocksDir + Path.SEPARATOR + blocksArray[index]);
        if (!appFS.exists(blockFilePath)) {
          throw new BlockNotFoundException(blockFilePath);
        }
        FileStatus status = appFS.getFileStatus(new Path(blocksDir + Path.SEPARATOR + blocksArray[index]));
        if (index == 0) {
          replicationFactor = status.getReplication();
          LOG.debug("replicationFactor: {}", replicationFactor);
        } else {
          sameReplicationFactor = (replicationFactor == status.getReplication());
          LOG.debug("sameReplicationFactor: {}", sameReplicationFactor);
        }

        if (index != numBlocks - 1) {
          multipleOfBlockSize = (status.getLen() % defaultBlockSize == 0);
          LOG.debug("multipleOfBlockSize: {}", multipleOfBlockSize);
        }
      }
      return sameReplicationFactor && multipleOfBlockSize;
    }
  }

}
