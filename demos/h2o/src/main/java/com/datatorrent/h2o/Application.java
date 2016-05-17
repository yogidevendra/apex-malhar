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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

@ApplicationAnnotation(name = "PredictiveMaintainceForWindTurbines")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    InputFileReader input = dag.addOperator("Input", new InputFileReader());
    Enricher enricher = dag.addOperator("Enricher", new Enricher());
    H2OScorer scorer = dag.addOperator("H2OScorer", new H2OScorer());

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");

    AppDataSnapshotServerMap snapshotServer = dag.addOperator("SnapshotServer", new AppDataSnapshotServerMap());

    String snapshotServerJSON = SchemaUtils.jarResourceFileToString("schema.json");
    snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);

    PubSubWebSocketAppDataQuery wsQuery = new PubSubWebSocketAppDataQuery();
    wsQuery.setTopic("SnapshotExampleQuery");
    wsQuery.setUri(uri);
    snapshotServer.setEmbeddableQueryInfoProvider(wsQuery);

    PubSubWebSocketAppDataResult wsResult = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsResult.setUri(uri);
    wsResult.setTopic("SnapshotExampleResult");

    dag.addStream("enrich", input.output, enricher.input);
    dag.addStream("score", enricher.output, scorer.input);
    dag.addStream("report", scorer.output, snapshotServer.input);
    dag.addStream("Result", snapshotServer.queryResult, wsResult.input);
  }
}