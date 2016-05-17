/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.h2o;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class InputFileReader extends AbstractFileInputOperator<String>
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
  protected transient BufferedReader br;
  private boolean emitTuples;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {

    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    emitTuples = true;
  }

  @Override
  public void emitTuples()
  {
    if (emitTuples) {
      super.emitTuples();
      emitTuples = false;
    }
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    //pendingFiles.remove(currentFile);
    pendingFiles.add(currentFile);
    super.closeFile(is);
    br = null;
  }

  @Override
  protected String readEntity() throws IOException
  {
    return br.readLine();
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }
}
