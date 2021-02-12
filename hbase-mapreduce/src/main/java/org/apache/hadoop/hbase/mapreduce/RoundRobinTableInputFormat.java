/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Process the lists that come back from {@link TableInputFormat} reordering so splits don't
 * clump around Servers; try and spread the splits around the cluster.
 * TIF returns splits in hbase:meta table order. Adjacent or near-adjacent
 * Regions could be hosted on the same RegionServer. Task placement therefore
 * could be lumpy with some RegionServers serving lots of inputs where other
 * servers could be serving few or idle. See the below helpful Flipkart blog post for
 * a description and from where the base of this code comes from.
 * @see https://tech.flipkart.com/is-data-locality-always-out-of-the-box-in-hadoop-not-really-2ae9c95163cb
 */
@InterfaceAudience.Public
public class RoundRobinTableInputFormat extends TableInputFormat {
  static List<InputSplit> roundRobin(List<InputSplit> inputs) throws IOException {
    if ((inputs == null) || inputs.isEmpty()) {
      return inputs;
    }
    List<InputSplit> roundRobinedSplits = new ArrayList<>(inputs.size());
    Map<String, List<InputSplit>> regionServerSplits = new HashMap<>();
    // Prepare a hashmap with each region server as key and list of Input Splits as value
    for (InputSplit is: inputs) {
      if (is instanceof TableSplit) {
        String regionServer = ((TableSplit)is).getRegionLocation();
        if (regionServer != null && !regionServer.isEmpty()) {
          regionServerSplits.computeIfAbsent(regionServer, k -> new LinkedList<>()).add(is);
          continue;
        }
      }
      // If TableSplit or region server not found, add it anyways.
      roundRobinedSplits.add(is);
    }
    // Now write out the splits in a manner that spreads the splits for a RegionServer as far apart
    // as possible.
    while (!regionServerSplits.isEmpty()) {
      Iterator<String> iterator = regionServerSplits.keySet().iterator();
      while (iterator.hasNext()) {
        String regionServer = iterator.next();
        List<InputSplit> inputSplitListForRegion = regionServerSplits.get(regionServer);
        if (!inputSplitListForRegion.isEmpty()) {
          roundRobinedSplits.add(inputSplitListForRegion.remove(0));
        }
        if (inputSplitListForRegion.isEmpty()) {
          iterator.remove();
        }
      }
    }
    // Finally, this code up in hadoop will mess up our nice sort:
    //  https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/JobSubmitter.java#L315
    // It compares splits using the hard-coded split comparator which just compares on split
    // lengths which here are region size -- it is trying to run the big Regions first... Only
    // locality is more important than size! We can't change the hardcoding so make the
    // SplitComparator sort respect our RR sort by setting length to be the array position in
    // reverse so whats first in our list looks like the longest length'd Region.
    int counter = roundRobinedSplits.size();
    List<InputSplit> result = new ArrayList<InputSplit>(counter);
    for (InputSplit is: roundRobinedSplits) {
      TableSplit tis = (TableSplit)is;
      result.add(new TableSplit(tis.getTable(), tis.getScan(), tis.getStartRow(),
        tis.getEndRow(), tis.getRegionLocation(), counter-- /*Decrement*/));
    }
    return result;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    return roundRobin(super.getSplits(context));
  }

  /**
   * Pass table name as argument. Set the zk ensemble to use with the System property
   * 'hbase.zookeeper.quorum'
   */
  public static void main(String[] args) throws IOException {
    TableInputFormat tif = new RoundRobinTableInputFormat();
    final Configuration configuration = HBaseConfiguration.create();
    configuration.setBoolean("hbase.regionsizecalculator.enable", false);
    configuration.set(HConstants.ZOOKEEPER_QUORUM,
      System.getProperty(HConstants.ZOOKEEPER_QUORUM, "localhost"));
    configuration.set(TableInputFormat.INPUT_TABLE, args[0]);
    tif.setConf(configuration);
    List<InputSplit> splits = tif.getSplits(new JobContextImpl(configuration, new JobID()));
    for (InputSplit split: splits) {
      System.out.println(split);
    }
  }
}
