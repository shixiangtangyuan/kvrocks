/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include "cluster/redis_slot.h"

enum {
  kClusterMaster = 1,
  kClusterSlave = 2,
  kClusterNodeIdLen = 40,
  kClusterPortIncr = 10000,
  kClusterSlots = HASH_SLOTS_SIZE,
};

inline constexpr const char *errInvalidNodeID = "Invalid cluster node id";
inline constexpr const char *errInvalidNodeServingStatus = "Invalid cluster node serving status";
inline constexpr const char *errInvalidSlotID = "Invalid slot id";
inline constexpr const char *errSlotOutOfRange = "Slot is out of range";
inline constexpr const char *errInvalidClusterVersion = "Invalid cluster version";
inline constexpr const char *errInvalidClusterRole = "Invalid cluster role";
inline constexpr const char *errInvalidClusterPool = "Invalid cluster Pool";
inline constexpr const char *errClusterIDNotMatch = "Cluster id does not match";
inline constexpr const char *errSlotOverlapped = "Slot distribution is overlapped";
inline constexpr const char *errSlotNotEnough = "Slot distribution is not enough";
inline constexpr const char *errInvalidMySlotRanges = "Invalid slotrange for myself";
inline constexpr const char *errInvalidDBID = "Invalid DBId";
inline constexpr const char *errSlotRangeNotMatchWithServing = "Slotrange not match with serving node";
inline constexpr const char *errSlotRangeNotMatchWithImporting = "Slotrange not match with importing node";
inline constexpr const char *errSlotRangeExists = "Slotrange already exists";
inline constexpr const char *errWrongNodeTopoStatus = "Wrong topo status for this datanode";
inline constexpr const char *errClusterNoInitialized = "CLUSTERDOWN The cluster is not initialized";
inline constexpr const char *errInvalidImportState = "Invalid import state";
inline constexpr const char *errDatanodeNotFoundInTopo = "Datanode not found in topo";
