package com.datatorrent.lib.io.fs;

import java.io.IOException;

import org.junit.Test;

public class HDFSOutputOperatorTest extends AbstractFileOutputOperatorTest
{
  @Test
  public void testIdleWindowsFinalize() throws IOException
  {
    HDFSOutputOperator writer = new HDFSOutputOperator();
    writer.setFileName("output.txt");
    writer.setFilePath(testMeta.getDir());
    writer.setAlwaysWriteToTmp(false);
    writer.setMaxIdleWindows(5);
    writer.setup(testMeta.testOperatorContext);

    String [][] tuples = {
        {"0a", "0b"},
        {"1a", "1b"},
        {},
        {},
        {},
        {},
        {"6a", "6b"},
        {"7a", "7b"},
        {},
        {},
        {},
        {},
        {},
        {"13a", "13b"},
        {"14a", "14b"},
        {},
        {},
        {},
        {"18a", "18b"},
        {"19a", "19b"},
        {},
        {},
        {},
        {},
        {},
        {},
        {"26a", "26b"}
    };
    
    for (int i = 0; i <= 12; i++) {
      writer.beginWindow(i);
      for(String t : tuples[i]){
        writer.stringInput.put(t);
      }
      writer.endWindow();
    }
    writer.committed(10);
    
    for (int i = 13; i <= 23; i++) {
      writer.beginWindow(i);
      for(String t : tuples[i]){
        writer.stringInput.put(t);
      }
      writer.endWindow();
    }
    writer.committed(20);
    writer.committed(25);
  }
}
