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
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.dimensions.GenericDimensionsComputationSingleSchema.DimensionsConversionContext;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIncrementalAggregator implements IncrementalAggregator
{
  private static final long serialVersionUID = 201506211153L;

  protected DimensionsConversionContext context;
  protected IndexSubset indexSubsetAggregates;
  protected IndexSubset indexSubsetKeys;

  public AbstractIncrementalAggregator()
  {
  }

  @Override
  public void setDimensionsConversionContext(DimensionsConversionContext context)
  {
    this.context = Preconditions.checkNotNull(context);
  }

  @Override
  public void setIndexSubsetKeys(IndexSubset indexSubsetKeys)
  {
    this.indexSubsetKeys = Preconditions.checkNotNull(indexSubsetKeys);
  }

  @Override
  public void setIndexSubsetAggregates(IndexSubset indexSubsetAggregates)
  {
    this.indexSubsetAggregates = Preconditions.checkNotNull(indexSubsetAggregates);
  }

  @Override
  public Aggregate getGroup(InputEvent src, int aggregatorIndex)
  {
    Aggregate aggregate = createAggregate(src,
                                          context,
                                          indexSubsetAggregates,
                                          indexSubsetKeys,
                                          aggregatorIndex);
    aggregate.setAggregatorIndex(aggregatorIndex);
    GPOUtils.indirectCopy(aggregate.getAggregates(), src.getAggregates(), indexSubsetAggregates);
    return aggregate;
  }

  @Override
  public int computeHashCode(InputEvent inputEvent)
  {
    return GPOUtils.indirectHashcode(inputEvent.getKeys(), indexSubsetKeys);
  }

  @Override
  public boolean equals(InputEvent inputEvent1, InputEvent inputEvent2)
  {
    InputEvent inputEvent;
    InputEvent aggregate;

    if(inputEvent1.isTypeInputEvent()) {
      inputEvent = inputEvent1;
      aggregate = inputEvent2;
    }
    else {
      inputEvent = inputEvent2;
      aggregate = inputEvent1;
    }

    return GPOUtils.indirectEquals(aggregate.getKeys(),
                                   inputEvent.getKeys(),
                                   indexSubsetKeys);
  }

  public static Aggregate createAggregate(InputEvent inputEvent,
                                          DimensionsConversionContext context,
                                          IndexSubset indexSubsetAggregates,
                                          IndexSubset indexSubsetKeys,
                                          int aggregatorIndex)
  {
    GPOMutable aggregates = new GPOMutable(context.aggregateDescriptor);
    GPOMutable keys = new GPOMutable(context.keyDescriptor);

    LOG.debug("indexSubsetAggregates {}", indexSubsetAggregates);
    GPOUtils.indirectCopy(aggregates, inputEvent.getAggregates(), indexSubsetAggregates);
    LOG.debug("copied {}", aggregates);
    LOG.debug("time val {} {}", inputEvent.getKeys().getFieldLong("time"), indexSubsetKeys.fieldsLongIndexSubset);
    GPOUtils.indirectCopy(keys, inputEvent.getKeys(), indexSubsetKeys);
    LOG.debug("time val {} {}", keys.getFieldLong("time"), indexSubsetKeys.fieldsLongIndexSubset);

    if(context.outputTimebucketIndex >= 0) {
      TimeBucket timeBucket = context.dd.getTimeBucket();
      LOG.debug("time bucket {}", timeBucket);

      keys.getFieldsInteger()[context.outputTimebucketIndex] = timeBucket.ordinal();
      LOG.debug("before round down {}", keys.getFieldsLong()[context.outputTimestampIndex]);
      keys.getFieldsLong()[context.outputTimestampIndex] =
      timeBucket.roundDown(inputEvent.getKeys().getFieldsLong()[context.inputTimestampIndex]);
      LOG.debug("before round down {}", keys.getFieldsLong()[context.outputTimestampIndex]);
    }

    EventKey eventKey = new EventKey(context.schemaID,
                                     context.dimensionsDescriptorID,
                                     context.aggregatorID,
                                     keys);

    return new Aggregate(eventKey,
                         aggregates);
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIncrementalAggregator.class);
}
