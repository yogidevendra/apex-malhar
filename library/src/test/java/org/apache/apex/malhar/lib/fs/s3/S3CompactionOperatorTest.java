package org.apache.apex.malhar.lib.fs.s3;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;

public class S3CompactionOperatorTest
{

  private class TestMeta extends TestWatcher
  {
    S3CompactionOperator underTest;
    Context.OperatorContext context;
    String outputPath;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      outputPath = new File("target/" + description.getClassName() + "/" + description.getMethodName()).getPath();

      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, description.getClassName());
      attributes.put(DAG.DAGContext.APPLICATION_PATH, outputPath);
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);

      underTest = new S3CompactionOperator<byte[]>();
      underTest.setConverter(new GenericFileOutputOperator.NoOpConverter());
      underTest.setup(context);
      underTest.setMaxIdleWindows(10);
    }

    @Override
    protected void finished(Description description)
    {
      this.underTest.teardown();
      try {
        FileUtils.deleteDirectory(new File("target" + Path.SEPARATOR + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testRotate() throws Exception
  {
    CollectorTestSink<S3Reconciler.OutputMetaData> sink = new CollectorTestSink<S3Reconciler.OutputMetaData>();
    testMeta.underTest.output.setSink((CollectorTestSink)sink);

    for (int i = 0; i < 60; i++) {
      testMeta.underTest.beginWindow(i);
      if (i < 10) {
        testMeta.underTest.input.process(("Record" + Integer.toString(i)).getBytes());
      }
      testMeta.underTest.endWindow();
    }
    testMeta.underTest.committed(59);

    Assert.assertEquals("s3-compaction_1.0", sink.collectedTuples.get(0).getFileName());
  }
}
