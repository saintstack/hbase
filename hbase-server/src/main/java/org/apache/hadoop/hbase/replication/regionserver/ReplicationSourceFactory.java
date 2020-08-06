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
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs appropriate {@link ReplicationSourceInterface}.
 * Considers whether Recovery or not, whether hbase:meta Region Read Replicas or not, etc.
 */
@InterfaceAudience.Private
public final class ReplicationSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceFactory.class);

  private ReplicationSourceFactory() {}

  static ReplicationSourceInterface create(Configuration conf, String queueId) {
    // Check for the marker used to enable replication of hbase:meta for region read replicas.
    // There is no queue recovery when hbase:meta region read replicas so no need to check zk.
    if (ServerRegionReplicaUtil.isMetaRegionReplicaReplicationPeer(queueId)) {
      // Create ReplicationSource that only reads hbase:meta WAL files and that lets through
      // all WALEdits found in these WALs.
      return new ReplicationSource(p -> AbstractFSWALProvider.isMetaFile(p),
        Collections.emptyList());
    }
    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(queueId);
    boolean queueRecovered = replicationQueueInfo.isQueueRecovered();
    String defaultReplicationSourceImpl = null;
    Class<?> c = null;
    try {
      defaultReplicationSourceImpl = queueRecovered?
        RecoveredReplicationSource.class.getCanonicalName():
        ReplicationSource.class.getCanonicalName();
      c = Class.forName(conf.get("replication.replicationsource.implementation",
        defaultReplicationSourceImpl));
      return c.asSubclass(ReplicationSourceInterface.class).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.warn("Configured replication class {} failed construction, defaulting to {}",
        c.getName(), defaultReplicationSourceImpl, e);
      return queueRecovered? new RecoveredReplicationSource(): new ReplicationSource();
    }
  }
}
