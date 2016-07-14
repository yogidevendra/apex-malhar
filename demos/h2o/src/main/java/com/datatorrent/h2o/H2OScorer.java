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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import hex.genmodel.GenModel;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

public class H2OScorer extends BaseOperator {
	private static String modelClassName = "glm_f25f6cc8_3665_49dc_82c1_eaf0be1dd391";
	private transient EasyPredictModelWrapper model;

	public final transient DefaultOutputPort<List<Map<String, Object>>> output = new DefaultOutputPort<>();
	public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
		@Override
		public void process(String s) {

			RowData row = new RowData();
			row.put("Year", "1987");
			row.put("Month", "10");
			row.put("DayofMonth", "14");
			row.put("DayOfWeek", "3");
			row.put("CRSDepTime", "730");
			row.put("UniqueCarrier", "PS");
			row.put("Origin", "SAN");
			row.put("Dest", "SFO");

			String[] splits = s.split(",");
			double[] data = new double[splits.length - 17];
			for (int i = 17; i < splits.length; i++) {
				data[i - 17] = Double.parseDouble(splits[i]);
			}
			double[] pred = new double[1];
			model.score0(data, pred);
			double absPred = Math.abs(pred[0]);
			if (absPred <= 50) {
				cyclesCount[0]++;
			} else if (absPred <= 100) {
				cyclesCount[1]++;
			} else if (absPred < 150) {
				cyclesCount[2]++;
			} else {
				cyclesCount[3]++;
			}
		}
	};

	@Override
	public void endWindow() {
		super.endWindow();
		List<Map<String, Object>> results = Lists.newArrayList();
		Map<String, Object> resultMap = Maps.newHashMap();
		resultMap.put("Remaining Cycles", "[0-50] cycles");
		resultMap.put("count", cyclesCount[0]);
		results.add(resultMap);
		resultMap = Maps.newHashMap();
		resultMap.put("Remaining Cycles", "(50-100] cycles");
		resultMap.put("count", cyclesCount[1]);
		results.add(resultMap);
		resultMap = Maps.newHashMap();
		resultMap.put("Remaining Cycles", "(100-150] cycles");
		resultMap.put("count", cyclesCount[2]);
		results.add(resultMap);
		resultMap = Maps.newHashMap();
		resultMap.put("Remaining Cycles", "(150- ] cycles");
		resultMap.put("count", cyclesCount[3]);
		results.add(resultMap);
		output.emit(results);
	}

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
}