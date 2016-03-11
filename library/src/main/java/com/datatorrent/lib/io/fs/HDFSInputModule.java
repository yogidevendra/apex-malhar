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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.block.BlockReader;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.fs.HDFSFileSplitter.HDFSScanner;
import com.datatorrent.netlet.util.Slice;

/**
 * HDFSInputModule is used to read files/list of files (or directory) from HDFS. <br/>
 * Module emits, <br/>
 * 1. FileMetadata 2. BlockMetadata 3. Block Bytes.<br/><br/>
 * The module reads data in parallel, following parameters can be configured<br/>
 * 1. files: list of file(s)/directories to read<br/>
 * 2. filePatternRegularExp: Files names matching given regex will be read<br/>
 * 3. scanIntervalMillis: interval between two scans to discover new files in input directory<br/>
 * 4. recursive: if scan recursively input directories<br/>
 * 5. blockSize: block size used to read input blocks of file<br/>
 * 6. readersCount: count of readers to read input file<br/>
 * 7. sequencialFileRead: If emit file blocks in sequence?
 */
public class HDFSInputModule implements Module
{

  @NotNull
  @Size(min = 1)
  private String files;
  private String filePatternRegularExp;
  @Min(0)
  private long scanIntervalMillis;
  private boolean recursive = true;
  private long blockSize;
  private boolean sequencialFileRead = false;
  private int readersCount;

  public final transient ProxyOutputPort<FileMetadata> filesMetadataOutput = new ProxyOutputPort<>();
  public final transient ProxyOutputPort<FileBlockMetadata> blocksMetadataOutput = new ProxyOutputPort<>();
  public final transient ProxyOutputPort<ReaderRecord<Slice>> messages = new ProxyOutputPort<>();

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    HDFSFileSplitter fileSplitter = dag.addOperator("FileSplitter", new HDFSFileSplitter());
    BlockReader blockReader = dag.addOperator("BlockReader", new BlockReader());

    dag.addStream("BlockMetadata", fileSplitter.blocksMetadataOutput, blockReader.blocksMetadataInput);

    filesMetadataOutput.set(fileSplitter.filesMetadataOutput);
    blocksMetadataOutput.set(blockReader.blocksMetadataOutput);
    messages.set(blockReader.messages);

    fileSplitter.setSequencialFileRead(sequencialFileRead);
    if (blockSize != 0) {
      fileSplitter.setBlockSize(blockSize);
    }

    HDFSScanner fileScanner = (HDFSScanner)fileSplitter.getScanner();
    fileScanner.setFiles(files);
    if (scanIntervalMillis != 0) {
      fileScanner.setScanIntervalMillis(scanIntervalMillis);
    }
    fileScanner.setRecursive(recursive);
    if (filePatternRegularExp != null) {
      fileSplitter.getScanner().setFilePatternRegularExp(filePatternRegularExp);
    }

    blockReader.setUri(files);
    if (readersCount != 0) {
      dag.setAttribute(blockReader, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<BlockReader>(readersCount));
    }
  }

  /**
   * A comma separated list of directories to scan. If the path is not fully qualified the default file system is used.
   * A fully qualified path can be provided to scan directories in other filesystems.
   *
   * @param files
   *          files
   */
  public void setFiles(String files)
  {
    this.files = files;
  }

  /**
   * Gets the files to be scanned.
   *
   * @return files to be scanned.
   */
  public String getFiles()
  {
    return files;
  }

  /**
   * Gets the regular expression for file names to split
   *
   * @return regular expression
   */
  public String getFilePatternRegularExp()
  {
    return filePatternRegularExp;
  }

  /**
   * Only files with names matching the given java regular expression are split
   *
   * @param filePatternRegexp
   *          regular expression
   */
  public void setFilePatternRegularExp(String filePatternRegexp)
  {
    this.filePatternRegularExp = filePatternRegexp;
  }

  /**
   * Gets scan interval in milliseconds, interval between two scans to discover new files in input directory
   *
   * @return scanInterval milliseconds
   */
  public long getScanIntervalMillis()
  {
    return scanIntervalMillis;
  }

  /**
   * Sets scan interval in milliseconds, interval between two scans to discover new files in input directory
   *
   * @param scanIntervalMillis
   */
  public void setScanIntervalMillis(long scanIntervalMillis)
  {
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * Get is scan recursive
   *
   * @return isRecursive
   */
  public boolean isRecursive()
  {
    return recursive;
  }

  /**
   * set is scan recursive
   *
   * @param recursive
   */
  public void setRecursive(boolean recursive)
  {
    this.recursive = recursive;
  }

  /**
   * Get block size used to read input blocks of file
   *
   * @return blockSize
   */
  public long getBlockSize()
  {
    return blockSize;
  }

  /**
   * Sets block size used to read input blocks of file
   *
   * @param blockSize
   */
  public void setBlockSize(long blockSize)
  {
    this.blockSize = blockSize;
  }

  /**
   * Gets readers count
   * @return readersCount
   */
  public int getReadersCount()
  {
    return readersCount;
  }

  /**
   * Static count of readers to read input file
   * @param readersCount
   */
  public void setReadersCount(int readersCount)
  {
    this.readersCount = readersCount;
  }

  /**
   * Gets is sequencial file read
   * 
   * @return sequencialFileRead
   */
  public boolean isSequencialFileRead()
  {
    return sequencialFileRead;
  }

  /**
   * Sets is sequencial file read
   *
   * @param sequencialFileRead
   */
  public void setSequencialFileRead(boolean sequencialFileRead)
  {
    this.sequencialFileRead = sequencialFileRead;
  }

}
