package com.datatorrent.lib.io.output;

import java.io.File;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.block.BlockMetadata.FileBlockMetadata;
import com.datatorrent.lib.io.fs.AbstractFileSplitter.FileMetadata;
import com.datatorrent.lib.io.output.Synchronizer.OutputFileMetadata;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class SynchronizerTest
{
  public static final String[] FILE_NAMES = { "a.txt", "b.txt", "c.txt", "d.txt", "e.txt" };

  public static final long[][] BLOCK_IDS = {
      //Block ids for file1 (a.txt) 
      { 1001, 1002, 1003 },
      //Block ids for file2 (b.txt)
      { 1004, 1005, 1006, 1007 },
      //c.txt
      { 1008, 1009, 1010 },
      //d.txt
      { 1011, 1012 },
      //e.txt
      { 1013, 1014 } };

  List<FileMetadata> fileMetadataList;

  List<FileBlockMetadata> blockMetadataList;
  
  Synchronizer underTest;

  public SynchronizerTest()
  {

    underTest = new Synchronizer();
    fileMetadataList = Lists.newArrayList();
    blockMetadataList = Lists.newArrayList();
    
    for (int i = 0; i < FILE_NAMES.length; i++) {
      FileMetadata fileMetadata = new FileMetadata(FILE_NAMES[i]);
      fileMetadata.setFileName(FILE_NAMES[i]);
      fileMetadata.setBlockIds(BLOCK_IDS[i]);
      fileMetadata.setNumberOfBlocks(BLOCK_IDS[i].length);

      for (int blockIndex = 0; blockIndex < BLOCK_IDS[i].length; blockIndex++) {
        FileBlockMetadata fileBlockMetadata = new FileBlockMetadata(FILE_NAMES[i]);
        fileBlockMetadata.setBlockId(BLOCK_IDS[i][blockIndex]);
        blockMetadataList.add(fileBlockMetadata);
      }

      fileMetadataList.add(fileMetadata);
    }
  }

  
  
  
  @Test
  public void testSynchronizer()
  {
    
    CollectorTestSink<OutputFileMetadata> sink = new CollectorTestSink<OutputFileMetadata>();
    underTest.trigger.setSink((CollectorTestSink) sink);
    
    underTest.filesMetadataInput.process(fileMetadataList.get(0));
    Assert.assertEquals(0, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(0));
    underTest.blocksMetadataInput.process(blockMetadataList.get(1));
    Assert.assertEquals(0, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(2));
    Assert.assertEquals(1, sink.collectedTuples.size());
    Assert.assertEquals("a.txt", sink.collectedTuples.get(0).getFileName());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(3));
    underTest.blocksMetadataInput.process(blockMetadataList.get(4));
    Assert.assertEquals(1, sink.collectedTuples.size());

    underTest.filesMetadataInput.process(fileMetadataList.get(1));
    Assert.assertEquals(1, sink.collectedTuples.size());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(5));
    Assert.assertEquals(1, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(6));
    Assert.assertEquals(2, sink.collectedTuples.size());
    Assert.assertEquals("b.txt", sink.collectedTuples.get(1).getFileName());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(7));
    underTest.blocksMetadataInput.process(blockMetadataList.get(8));
    Assert.assertEquals(2, sink.collectedTuples.size());
    underTest.blocksMetadataInput.process(blockMetadataList.get(9));
    Assert.assertEquals(2, sink.collectedTuples.size());
    
    
    
    underTest.filesMetadataInput.process(fileMetadataList.get(2));
    Assert.assertEquals(3, sink.collectedTuples.size());
    Assert.assertEquals("c.txt", sink.collectedTuples.get(2).getFileName());
    
    underTest.filesMetadataInput.process(fileMetadataList.get(3));
    underTest.filesMetadataInput.process(fileMetadataList.get(4));
    Assert.assertEquals(3, sink.collectedTuples.size());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(10));
    Assert.assertEquals(3, sink.collectedTuples.size());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(11));
    
    Assert.assertEquals(4, sink.collectedTuples.size());
    Assert.assertEquals("d.txt", sink.collectedTuples.get(3).getFileName());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(12));
    Assert.assertEquals(4, sink.collectedTuples.size());
    
    underTest.blocksMetadataInput.process(blockMetadataList.get(13));
    Assert.assertEquals(5, sink.collectedTuples.size());
    Assert.assertEquals("e.txt", sink.collectedTuples.get(4).getFileName());
    
  }

}
