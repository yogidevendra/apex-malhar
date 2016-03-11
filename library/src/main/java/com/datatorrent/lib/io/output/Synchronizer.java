/*
 * Copyright (c) 2016 DataTorrent, Inc. 
 * ALL Rights Reserved.
 *
 */
package com.datatorrent.lib.io.output;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.block.BlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.fs.HDFSFileSplitter.HDFSFileMetaData;
import com.datatorrent.lib.io.output.OutputFileMetaData.OutputBlock;
import com.datatorrent.lib.io.output.OutputFileMetaData.OutputFileBlockMetaData;

/**
 * <p>
 * Synchronizer class.
 * </p>
 *
 */
public class Synchronizer extends BaseOperator
{
  private Map<String, FileMetadata> fileMetadataMap = Maps.newHashMap();
  private Map<String, Map<Long, BlockMetadata.FileBlockMetadata>> fileToReceivedBlocksMetadataMap = Maps.newHashMap();

  public Synchronizer()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
  }

  public final transient DefaultInputPort<FileMetadata> filesMetadataInput = new DefaultInputPort<FileMetadata>()
  {
    @Override
    public void process(FileMetadata fileMetadata)
    {
      String filePath = fileMetadata.getFilePath();
      Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = getReceivedBlocksMetadata(filePath);
      fileMetadataMap.put(filePath, fileMetadata);
      emitTriggerIfAllBlocksReceived(fileMetadata, receivedBlocksMetadata);
    }
  };

  public final transient DefaultInputPort<BlockMetadata.FileBlockMetadata> blocksMetadataInput = new DefaultInputPort<BlockMetadata.FileBlockMetadata>()
  {
    @Override
    public void process(BlockMetadata.FileBlockMetadata blockMetadata)
    {
      String filePath = blockMetadata.getFilePath();
      LOG.debug("received blockId {} for file {}", blockMetadata.getBlockId(), filePath);

      Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = getReceivedBlocksMetadata(filePath);
      receivedBlocksMetadata.put(blockMetadata.getBlockId(), blockMetadata);
      FileMetadata fileMetadata = fileMetadataMap.get(filePath);
      if (fileMetadata != null) {
        emitTriggerIfAllBlocksReceived(fileMetadata, receivedBlocksMetadata);
      }

    }
  };

  private void emitTriggerIfAllBlocksReceived(FileMetadata fileMetadata,
      Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata)
  {
    String filePath = fileMetadata.getFilePath();

    if (receivedBlocksMetadata.size() != fileMetadata.getNumberOfBlocks()) {
      //Some blocks are yet to be received
      fileMetadataMap.put(filePath, fileMetadata);
    } else {
      //No of received blocks match number of file blocks
      Set<Long> receivedBlocks = receivedBlocksMetadata.keySet();

      boolean blockMissing = false;
      for (long blockId : fileMetadata.getBlockIds()) {
        if (!receivedBlocks.contains(blockId)) {
          blockMissing = true;
        }
      }

      if (!blockMissing) {
        //All blocks received emit the filemetadata
        long fileProcessingTime = System.currentTimeMillis() - fileMetadata.getDiscoverTime();
        List<OutputBlock> outputBlocks = constructOutputBlockMetadataList(fileMetadata);
        ModuleFileMetaData moduleFileMetaData = new ModuleFileMetaData(fileMetadata, outputBlocks);
        trigger.emit(moduleFileMetaData);
        LOG.debug("Total time taken to process the file {} is {} ms", fileMetadata.getFilePath(), fileProcessingTime);
        fileMetadataMap.remove(filePath);
      }
    }
  }

  private List<OutputBlock> constructOutputBlockMetadataList(FileMetadata fileMetadata)
  {
    String filePath = fileMetadata.getFilePath();
    Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = fileToReceivedBlocksMetadataMap.get(filePath);
    List<OutputBlock> outputBlocks = Lists.newArrayList();
    long[] blockIDs = fileMetadata.getBlockIds();
    for (int i = 0; i < blockIDs.length; i++) {
      Long blockId = blockIDs[i];
      OutputFileBlockMetaData outputFileBlockMetaData = new OutputFileBlockMetaData(receivedBlocksMetadata.get(blockId),
          fileMetadata.getRelativePath(), (i == blockIDs.length - 1));
      outputBlocks.add(outputFileBlockMetaData);
    }
    return outputBlocks;

  }

  private Map<Long, BlockMetadata.FileBlockMetadata> getReceivedBlocksMetadata(String filePath)
  {
    Map<Long, BlockMetadata.FileBlockMetadata> receivedBlocksMetadata = fileToReceivedBlocksMetadataMap.get(filePath);
    if (receivedBlocksMetadata == null) {
      //No blocks received till now
      receivedBlocksMetadata = fileToReceivedBlocksMetadataMap.put(filePath,
          new HashMap<Long, BlockMetadata.FileBlockMetadata>());
    }
    return receivedBlocksMetadata;
  }

  public static class ModuleFileMetaData extends HDFSFileMetaData implements OutputFileMetaData
  {
    private List<OutputBlock> outputBlockMetaDataList;

    protected ModuleFileMetaData()
    {
      super();
      outputBlockMetaDataList = Lists.newArrayList();
    }

    protected ModuleFileMetaData(FileMetadata fileMetaData, List<OutputBlock> outputBlockMetaDataList)
    {
      super();

      for (int i = 0; i < SUPER_GETTER_METHODS.size(); i++) {
        try {
          THIS_SETTER_METHODS.get(i).invoke(this, SUPER_GETTER_METHODS.get(i).invoke(fileMetaData));
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          throw new RuntimeException("Error in initializing ModuleFileMetaData", e);
        }
      }
      this.outputBlockMetaDataList = outputBlockMetaDataList;
    }

    public ModuleFileMetaData(@NotNull String filePath)
    {
      super(filePath);
    }

    public String getOutputRelativePath()
    {
      return relativePath;
    }

    /* (non-Javadoc)
     * @see com.datatorrent.apps.ingestion.io.output.OutputFileMetaData#getOutputBlocksList()
     */
    @Override
    public List<OutputBlock> getOutputBlocksList()
    {
      return outputBlockMetaDataList;
    }

    /**
     * @param outputBlockMetaDataList
     *          the outputBlockMetaDataList to set
     */
    public void setOutputBlockMetaDataList(List<OutputBlock> outputBlockMetaDataList)
    {
      this.outputBlockMetaDataList = outputBlockMetaDataList;
    }

    //Collecting getter methods from super class
    private static final List<Method> SUPER_GETTER_METHODS;

    //Collecting setter methods from this class
    private static final List<Method> THIS_SETTER_METHODS;

    static {
      //Initialization of static lists
      SUPER_GETTER_METHODS = Lists.newArrayList();
      THIS_SETTER_METHODS = Lists.newArrayList();

      Method[] methods = HDFSFileMetaData.class.getMethods();
      for (Method method : methods) {
        if (isGetter(method)) {
          SUPER_GETTER_METHODS.add(method);
          String setterName = "set" + method.getName().substring(3);
          try {
            Method setter = ModuleFileMetaData.class.getMethod(setterName, method.getReturnType());
            THIS_SETTER_METHODS.add(setter);
          } catch (NoSuchMethodException e) {
            throw new RuntimeException("Error in initializing ModuleFileMetaData", e);
          } catch (SecurityException e) {
            throw new RuntimeException("Error in initializing ModuleFileMetaData", e);
          }
        }
      }
    }

    public static boolean isGetter(Method method)
    {
      if (!method.getName().startsWith("get")) {
        return false;
      }
      if (method.getParameterTypes().length != 0) {
        return false;
      }
      if (void.class.equals(method.getReturnType())) {
        return false;
      }
      return true;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Synchronizer.class);
  public final transient DefaultOutputPort<ModuleFileMetaData> trigger = new DefaultOutputPort<ModuleFileMetaData>();
}
