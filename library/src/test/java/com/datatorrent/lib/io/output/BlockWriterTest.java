/*
 *  Copyright (c) 2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.io.output;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.block.AbstractBlockReader.ReaderRecord;
import com.datatorrent.netlet.util.Slice;


/**
 * Unit tests for BlockWriter
 */
public class BlockWriterTest
{
  
  public static final int BLOCK_SIZE = 5;
  
  public static final String[] FILE_CONTENTS = {
      "abcdefgh", "pqrst", "xyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "0123456789"};

  private class TestMeta extends TestWatcher
  {
    String outputPath;
    List<ReaderRecord<Slice>> blockDataList = Lists.newArrayList();
    Map<Long,String> blockIdToExpectedContent = Maps.newHashMap();
    
    BlockWriter underTest;
    File blocksDir;
    Context.OperatorContext context;
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#starting(org.junit.runner.Description)
     */
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File("target/" + description.getClassName() + "/" + description.getMethodName()).getPath();
      
      underTest = new BlockWriter();
      String appDirectory = outputPath;
      
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "PartitionWriterTest");
      attributes.put(DAG.DAGContext.APPLICATION_PATH, appDirectory);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      
      underTest.setup(context);
      
      try {
        File outDir = new File(outputPath);
        FileUtils.forceMkdir(outDir);
        
        blocksDir = new File(context.getValue(Context.DAGContext.APPLICATION_PATH) , BlockWriter.SUBDIR_BLOCKS);
        blocksDir.mkdirs();
        
        long blockID=1000;
        
        for (int i = 0; i < FILE_CONTENTS.length; i++) {
          for(int offset=0; offset< FILE_CONTENTS[i].length(); offset+= BLOCK_SIZE, blockID++){
            String blockContents;
            if(offset+BLOCK_SIZE < FILE_CONTENTS[i].length()){
              blockContents= FILE_CONTENTS[i].substring(offset, offset+BLOCK_SIZE);
            }
            else{
              blockContents= FILE_CONTENTS[i].substring(offset);
            }
            
            ReaderRecord<Slice> readerRecord = new ReaderRecord<Slice>(blockID, new Slice(blockContents.getBytes()));
            blockIdToExpectedContent.put(blockID, blockContents);
            blockDataList.add(readerRecord);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    /* (non-Javadoc)
     * @see org.junit.rules.TestWatcher#finished(org.junit.runner.Description)
     */
    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      
      try {
        FileUtils.deleteDirectory(new File(outputPath));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Rule
  public TestMeta testMeta = new TestMeta();
  
  @Test
  public void testBlockWriting() throws IOException{
    
    testMeta.underTest.beginWindow(0);
    int i=0;
    for(;i<=5;i++){
      testMeta.underTest.input.process(testMeta.blockDataList.get(i));
    }
    testMeta.underTest.endWindow();
    
    testMeta.underTest.beginWindow(1);
    for(;i<=10;i++){
      testMeta.underTest.input.process(testMeta.blockDataList.get(i));
    }
    testMeta.underTest.endWindow();
    
    testMeta.underTest.beginWindow(2);
    for(;i<testMeta.blockDataList.size();i++){
      testMeta.underTest.input.process(testMeta.blockDataList.get(i));
    }
    testMeta.underTest.endWindow();
    testMeta.underTest.committed(2);
    testMeta.underTest.teardown();
    
    File[] blockFileNames = testMeta.blocksDir.listFiles();
    for(File blockFile : blockFileNames ){
      Long blockId = Long.parseLong(blockFile.getName().split("\\.")[0]);
      String expected = testMeta.blockIdToExpectedContent.get(blockId);
      Assert.assertEquals(expected, FileUtils.readFileToString(blockFile));
    }
  }
  
  
}
