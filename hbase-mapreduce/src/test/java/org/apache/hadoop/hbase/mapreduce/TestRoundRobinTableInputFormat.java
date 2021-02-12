/**
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

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestRoundRobinTableInputFormat {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRoundRobinTableInputFormat.class);

  @Test
  public void testRoundRobinSplit() throws IOException, InterruptedException {
    String[] keys = {
      "aa", "ab", "ac", "ad", "ae",
      "ba", "bb", "bc", "bd", "be",
      "ca", "cb", "cc", "cd", "ce",
      "da", "db", "dc", "dd", "de",
      "ea", "eb", "ec", "ed", "ee",
      "fa", "fb", "fc", "fd", "fe",
      "ga", "gb", "gc", "gd", "ge",
      "ha", "hb", "hc", "hd", "he",
      "ia", "ib", "ic", "id", "ie",
      "ja", "jb", "jc", "jd", "je", "jf"
    };

    List<InputSplit> splits = new ArrayList<>(keys.length - 1);
    for (int i = 0; i < keys.length - 1; i++) {
      InputSplit split = new TableSplit(TableName.valueOf("test"), new Scan(), Bytes.toBytes(keys[i]),
        Bytes.toBytes(keys[i + 1]), String.valueOf(i % 5 + 1), "", 100);
      splits.add(split);
    }
    Collections.shuffle(splits);

    List<InputSplit> rrList = RoundRobinTableInputFormat.roundRobin(splits);

    for (int i = 0; i < keys.length/5; i ++) {
      int counts[] = new int[5];
      for (int j = i * 5; j < i * 5 + 5; j ++) {
        counts[Integer.valueOf(rrList.get(j).getLocations()[0]) - 1] ++;
      }
      for (int value : counts) {
        assertEquals(value, 1);
      }
    }
  }
}
