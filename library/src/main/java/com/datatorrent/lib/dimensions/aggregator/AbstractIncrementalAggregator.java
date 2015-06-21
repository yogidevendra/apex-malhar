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

package com.datatorrent.lib.dimensions.aggregator;

import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.gpo.GPOUtils.IndexSubset;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.GenericDimensionsComputation.DimensionsConversionContext;
import com.google.common.base.Preconditions;

public abstract class AbstractIncrementalAggregator implements IncrementalAggregator
{
  private static final long serialVersionUID = 201506211153L;

  protected DimensionsConversionContext context;
  protected IndexSubset indexSubset;

  public AbstractIncrementalAggregator()
  {
  }

  public void setDimensionsConversionContext(DimensionsConversionContext context)
  {
    this.context = Preconditions.checkNotNull(context);
  }

  public void setIndexSubset(IndexSubset indexSubset)
  {
    this.indexSubset = Preconditions.checkNotNull(indexSubset);
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);
    Aggregate aggregate = new Aggregate(context.eventKey,
                                        aggregates);
    aggregate.setAggregatorIndex(aggregatorIndex);
    GPOUtils.indirectCopy(aggregates, src.getAggregates(), indexSubset);
    return aggregate;
  }

  @Override
  public int computeHashCode(InputEvent inputEvent)
  {
    return inputEvent.getEventKey().hashCode();
  }

  @Override
  public boolean equals(InputEvent inputEvent1, InputEvent inputEvent2)
  {
    return inputEvent1.getEventKey().equals(inputEvent2.getEventKey());
  }
}
