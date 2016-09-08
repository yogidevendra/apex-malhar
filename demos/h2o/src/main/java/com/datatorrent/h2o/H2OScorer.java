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

import org.apache.commons.collections.keyvalue.MultiKey;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

import hex.genmodel.GenModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.AbstractPrediction;
import hex.genmodel.easy.prediction.BinomialModelPrediction;

public class H2OScorer extends BaseOperator {
	private static String modelClassName = "glm_f25f6cc8_3665_49dc_82c1_eaf0be1dd391";
	private transient EasyPredictModelWrapper model;

	public final transient DefaultOutputPort<MultiKey> output = new DefaultOutputPort<>();
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String s) {
		  
			RowData row = getRowData(s);
			AbstractPrediction prediction;
      try {
        prediction = model.predict(row);
        System.out.println("prediction ="+((BinomialModelPrediction)prediction).label);
        output.emit(new MultiKey(row, prediction));
      } catch (PredictException e) {
        e.printStackTrace();
      }
			
			
		}
	};

	@Override
	public void setup(Context.OperatorContext context) {
		super.setup(context);
		try {
			GenModel rawModel = (hex.genmodel.GenModel) Class.forName(modelClassName).newInstance();
			model = new EasyPredictModelWrapper(rawModel);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			DTThrowable.rethrow(e);
		}
	}
	
	public static void main(String[] args)
  {
	  H2OScorer scorer = new H2OScorer();
	  scorer.setup(null);
	  scorer.input.process(null);
  }
	
	public RowData getRowData(String s){
	  RowData row = new RowData();
    row.put("Year", "1987");
    row.put("Month", "10");
    row.put("DayofMonth", "14");
    row.put("DayOfWeek", "3");
    row.put("CRSDepTime", "730");
    row.put("UniqueCarrier", "PS");
    row.put("Origin", "SAN");
    row.put("Dest", "SFO");
    
    return row;
	}
}