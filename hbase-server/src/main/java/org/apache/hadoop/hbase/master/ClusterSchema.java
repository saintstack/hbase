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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * View and edit the current cluster schema.
 *
 * <h2>Implementation Notes</h2>
 * Nonces are for when operation is non-idempotent to ensure once-only semantic, even
 * across process failures.
 */
// ClusterSchema is private to the Master; only the Master knows current cluster state and has
// means of editing/altering it.
// TODO: Remove Server when MasterServices
@InterfaceAudience.Private
public interface ClusterSchema {
  /**
   * Timeout for cluster operations in milliseconds.
   */
  public static final String HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY =
      "hbase.master.cluster.schema.operation.timeout";
  /**
   * Default operation timeout in milliseconds.
   */
  public static final int DEFAULT_HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT =
      5 * 60 * 1000;

  /**
   * Utility method that will wait {@link #HBASE_MASTER_CLUSTER_SCHEMA_OPERATION_TIMEOUT_KEY}
   * timeout and if exceptions, does conversion so palatable outside Master: i.e.
   * {@link InterruptedException} becomes {@link InterruptedIOException} and so on.
   *
   * <<Utility>>
   *
   * @param future Future to wait on.
   * @return On completion, info on the procedure that ran.
   * @throws IOException
   */
  // TODO: Where to put this utility?
  ProcedureInfo get(final Future<ProcedureInfo> future) throws IOException;

  /**
   * For internals use only. Do not use. Provisionally part of this Interface.
   * Prefer the high-level APIs available elsewhere in this API.
   * @return Instance of {@link TableNamespaceManager}
   */
  // TODO: Remove from here. Keep internal. This Interface is too high-level to host this accessor.
  TableNamespaceManager getTableNamespaceManager();

  /**
   * Create a new Namespace.
   * @param namespaceDescriptor descriptor for new Namespace
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   * <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return Operation Future.
   * Use {@link Future#get(long, java.util.concurrent.TimeUnit)} to wait on completion.
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   * as well as {@link IOException}
   */
  Future<ProcedureInfo> createNamespace(NamespaceDescriptor namespaceDescriptor, long nonceGroup,
      long nonce)
  throws IOException;

  /**
   * Modify an existing Namespace.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   * <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return Operation Future.
   * Use {@link Future#get(long, java.util.concurrent.TimeUnit)} to wait on completion.
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   * as well as {@link IOException}
   */
  Future<ProcedureInfo> modifyNamespace(NamespaceDescriptor descriptor, long nonceGroup,
      long nonce)
  throws IOException;

  /**
   * Delete an existing Namespace.
   * Only empty Namespaces (no tables) can be removed.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   * <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return Operation Future.
   * Use {@link Future#get(long, java.util.concurrent.TimeUnit)} to wait on completion.
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   * as well as {@link IOException}
   */
  Future<ProcedureInfo> deleteNamespace(String name, long nonceGroup, long nonce)
  throws IOException;

  /**
   * Get a Namespace
   * @param name Name of the Namespace
   * @return Namespace descriptor for <code>name</code>
   * @throws IOException Throws {@link ClusterSchemaException} and {@link InterruptedIOException}
   * as well as {@link IOException}
   */
  // No Future here because presumption is that the request will go against cached metadata so
  // return immediately -- no need of running a Procedure.
  NamespaceDescriptor getNamespace(String name) throws IOException;

  /**
   * Get all Namespaces
   * @return All Namespace descriptors
   * @throws IOException
   */
  List<NamespaceDescriptor> getNamespaces() throws IOException;
}