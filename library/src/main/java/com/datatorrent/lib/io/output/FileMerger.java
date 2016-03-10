/*
 * Copyright (c) 2016 DataTorrent, Inc. 
 * ALL Rights Reserved.
 *
 */
package com.datatorrent.lib.io.output;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.input.ModuleFileSplitter.ModuleFileMetaData;

/**
 * This operator merges the blocks into a file. The list of blocks is obtained
 * from the IngestionFileMetaData. The implementation extends OutputFileMerger
 * (which uses reconsiler), hence the file merging operation is carried out in a
 * separate thread.
 *
 */
public class FileMerger extends FileStitcher<ModuleFileMetaData>
{
  private boolean overwriteOutputFile;

  private static final Logger LOG = LoggerFactory.getLogger(FileMerger.class);

  @AutoMetric
  private long bytesWrittenPerSec;

  private long bytesWritten;
  private double windowTimeSec;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    windowTimeSec = (context.getValue(Context.OperatorContext.APPLICATION_WINDOW_COUNT)
        * context.getValue(Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS) * 1.0) / 1000.0;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesWrittenPerSec = 0;
    bytesWritten = 0;
  }

  /* 
   * Calls super.endWindow() and sets counters 
   * @see com.datatorrent.api.BaseOperator#endWindow()
   */
  @Override
  public void endWindow()
  {
    ModuleFileMetaData tuple;
    int size = doneTuples.size();
    for (int i = 0; i < size; i++) {
      tuple = doneTuples.peek();
      // If a tuple is present in doneTuples, it has to be also present in successful/failed/skipped
      // as processCommittedData adds tuple in successful/failed/skipped
      // and then reconciler thread add that in doneTuples 
      if (successfulFiles.contains(tuple)) {
        successfulFiles.remove(tuple);
        LOG.debug("File copy successful: {}", tuple.getOutputRelativePath());
      } else if (skippedFiles.contains(tuple)) {
        skippedFiles.remove(tuple);
        LOG.debug("File copy skipped: {}", tuple.getOutputRelativePath());
      } else if (failedFiles.contains(tuple)) {
        failedFiles.remove(tuple);
        LOG.debug("File copy failed: {}", tuple.getOutputRelativePath());
      } else {
        throw new RuntimeException(
            "Tuple present in doneTuples but not in successfulFiles: " + tuple.getOutputRelativePath());
      }
      completedFilesMetaOutput.emit(tuple);
      committedTuples.remove(tuple);
      doneTuples.poll();
    }

    bytesWrittenPerSec = (long)(bytesWritten / windowTimeSec);
  }

  @Override
  protected void mergeOutputFile(ModuleFileMetaData moduleFileMetaData) throws IOException
  {
    LOG.debug("Processing file: {}", moduleFileMetaData.getOutputRelativePath());

    Path outputFilePath = new Path(filePath, moduleFileMetaData.getOutputRelativePath());
    if (moduleFileMetaData.isDirectory()) {
      createDir(outputFilePath);
      successfulFiles.add(moduleFileMetaData);
      return;
    }

    if (outputFS.exists(outputFilePath) && !overwriteOutputFile) {
      LOG.debug("Output file {} already exits and overwrite flag is off. Skipping.", outputFilePath);
      skippedFiles.add(moduleFileMetaData);
      return;
    }
    //Call super method for serial merge of blocks
    super.mergeOutputFile(moduleFileMetaData);

    Path destination = new Path(filePath, moduleFileMetaData.getOutputRelativePath());
    Path path = Path.getPathWithoutSchemeAndAuthority(destination);
    long len = outputFS.getFileStatus(path).getLen();
  }

  /* (non-Javadoc)
   * @see com.datatorrent.apps.ingestion.io.output.OutputFileMerger#writeTempOutputFile(com.datatorrent.apps.ingestion.io.output.OutputFileMetaData)
   */
  @Override
  protected OutputStream writeTempOutputFile(ModuleFileMetaData moduleFileMetadata)
      throws IOException, BlockNotFoundException
  {
    OutputStream outputStream = super.writeTempOutputFile(moduleFileMetadata);
    bytesWritten += moduleFileMetadata.getFileLength();
    return outputStream;
  }

  private void createDir(Path outputFilePath) throws IOException
  {
    if (!outputFS.exists(outputFilePath)) {
      outputFS.mkdirs(outputFilePath);
    }
  }

  @Override
  protected OutputStream getOutputStream(Path partFilePath) throws IOException
  {
    OutputStream outputStream = outputFS.create(partFilePath);
    return outputStream;
  }

  public boolean isOverwriteOutputFile()
  {
    return overwriteOutputFile;
  }

  public void setOverwriteOutputFile(boolean overwriteOutputFile)
  {
    this.overwriteOutputFile = overwriteOutputFile;
  }
}
