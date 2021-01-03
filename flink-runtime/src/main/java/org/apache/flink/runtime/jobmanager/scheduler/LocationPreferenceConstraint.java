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

package org.apache.flink.runtime.jobmanager.scheduler;

/**
 * Defines the location preference constraint.
 *
 * <p> Currently, we support that all input locations have to be taken into consideration
 * and only those which are known at scheduling time. Note that if all input locations
 * are considered, then the scheduling operation can potentially take a while until all
 * inputs have locations assigned.
 */
public enum LocationPreferenceConstraint {
	ALL, // wait for all inputs to have a location assigned
	ANY, // only consider those inputs who already have a location assigned
	PIPELINEBROKEN
}
