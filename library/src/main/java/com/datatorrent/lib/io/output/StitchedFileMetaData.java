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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;

/**
 * StitchedFileMetaData is used by FileStitcher to define constituents of the
 * output file. IngestionFileMetaData and PartitionMetaData are extended from
 * this class.
 *
 */
public interface StitchedFileMetaData
{
  /**
   * @return the outputFileName
   */
  public String getStitchedFileRelativePath();

  public List<StitchBlock> getStitchBlocksList();

  public interface StitchBlock
  {
    public void writeTo(FileSystem blocksFS, String blocksDir, OutputStream outputStream)
        throws IOException, BlockNotFoundException;

    public long getBlockId();
  }

  
  

/**
 * Defining new type of exception for missing block. Currently, methods catching
 * this exception assumes that block is missing because of explicit deletion by
 * Ingestion App (for completed files)
 *
 */
public static class BlockNotFoundException extends Exception
{

  private static final long serialVersionUID = -7409415466834194798L;

  Path blockPath;

  /**
   * @param blockPath
   */
  public BlockNotFoundException(Path blockPath)
  {
    super();
    this.blockPath = blockPath;
  }

  /**
   * @return the blockPath
   */
  public Path getBlockPath()
  {
    return blockPath;
  }

}

}
