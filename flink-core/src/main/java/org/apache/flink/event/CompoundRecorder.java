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

package org.apache.flink.event;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Compound recorder.
 */
public class CompoundRecorder implements AbstractEventRecorder{
	private final List<AbstractEventRecorder> recorderList = new ArrayList<>();

	public CompoundRecorder(WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder) {
		recorderList.add(warehouseJobStartEventMessageRecorder);
	}

	public CompoundRecorder(WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder, MetricEventRecorder metricEventRecorder) {
		recorderList.add(metricEventRecorder);
		recorderList.add(warehouseJobStartEventMessageRecorder);
	}

	public void setJobId (String jobId) {
		recordCompoundRecorderEvent(r -> r.setJobId(jobId));
	}

	public void buildProgramStart() {
		recordCompoundRecorderEvent(r -> r.buildProgramStart());
	}

	public void buildProgramFinish(){
		recordCompoundRecorderEvent(r -> r.buildProgramFinish());
	}

	public void buildStreamGraphStart(){
		recordCompoundRecorderEvent(r -> r.buildStreamGraphStart());
	}

	public void buildStreamGraphFinish(){
		recordCompoundRecorderEvent(r -> r.buildStreamGraphFinish());
	}

	public void buildJobGraphStart(){
		recordCompoundRecorderEvent(r -> r.buildJobGraphStart());
	}

	public void buildJobGraphFinish(){
		recordCompoundRecorderEvent(r -> r.buildJobGraphFinish());
	}

	public void prepareAMContextStart(){
		recordCompoundRecorderEvent(r -> r.prepareAMContextStart());
	}

	public void prepareAMContextFinish(){
		recordCompoundRecorderEvent(r -> r.prepareAMContextFinish());
	}

	public void deployYarnClusterStart(){
		recordCompoundRecorderEvent(r -> r.deployYarnClusterStart());
	}

	public void deployYarnClusterFinish(){
		recordCompoundRecorderEvent(r -> r.deployYarnClusterFinish());
	}

	public void submitJobStart(){
		recordCompoundRecorderEvent(r -> r.submitJobStart());
	}

	public void submitJobFinish(){
		recordCompoundRecorderEvent(r -> r.submitJobFinish());
	}

	public void checkSlotEnoughStart(){
		recordCompoundRecorderEvent(r -> r.checkSlotEnoughStart());
	}

	public void checkSlotEnoughFinish(){
		recordCompoundRecorderEvent(r -> r.checkSlotEnoughFinish());
	}

	//---------------------
	// K8s Client events
	//---------------------
	@Override
	public void buildDeployerStart() {
		recordCompoundRecorderEvent(r -> buildDeployerStart());
	}

	@Override
	public void buildDeployerFinish() {
		recordCompoundRecorderEvent(r -> buildDeployerFinish());
	}

	@Override
	public void uploadLocalFilesStart() {
		recordCompoundRecorderEvent(r -> uploadLocalFilesStart());
	}

	@Override
	public void uploadLocalFilesFinish() {
		recordCompoundRecorderEvent(r -> uploadLocalFilesFinish());
	}

	@Override
	public void deployApplicationClusterStart() {
		recordCompoundRecorderEvent(r -> deployApplicationClusterStart());
	}

	@Override
	public void deployApplicationClusterFinish() {
		recordCompoundRecorderEvent(r -> deployApplicationClusterFinish());
	}

	@Override
	public void downloadRemoteFilesStart() {
		recordCompoundRecorderEvent(r -> downloadRemoteFilesStart());
	}

	@Override
	public void downloadRemoteFilesFinish() {
		recordCompoundRecorderEvent(r -> downloadRemoteFilesFinish());
	}

	//---------------------
	// JobManager events
	//---------------------
	public void createSchedulerStart(){
		recordCompoundRecorderEvent(r -> r.createSchedulerStart());
	}

	public void createSchedulerFinish(){
		recordCompoundRecorderEvent(r -> r.createSchedulerFinish());
	}

	public void buildExecutionGraphStart(){
		recordCompoundRecorderEvent(r -> r.buildExecutionGraphStart());
	}

	public void buildExecutionGraphInitialization(){
		recordCompoundRecorderEvent(r -> r.buildExecutionGraphInitialization());
	}

	public void buildExecutionGraphAttachJobVertex(){
		recordCompoundRecorderEvent(r -> r.buildExecutionGraphAttachJobVertex());
	}

	public void buildExecutionGraphExecutionTopology(){
		recordCompoundRecorderEvent(r -> r.buildExecutionGraphExecutionTopology());
	}

	public void buildExecutionGraphFinish(){
		recordCompoundRecorderEvent(r -> r.buildExecutionGraphFinish());
	}

	public void scheduleTaskStart(long globalModVersion){
		recordCompoundRecorderEvent(r -> r.scheduleTaskStart(globalModVersion));
	}

	public void scheduleTaskAllocateResource(long globalModVersion){
		recordCompoundRecorderEvent(r -> r.scheduleTaskAllocateResource(globalModVersion));
	}

	public void scheduleTaskFinish(long globalModVersion){
		recordCompoundRecorderEvent(r -> r.scheduleTaskFinish(globalModVersion));
	}

	public void deployTaskStart(long globalModVersion){
		recordCompoundRecorderEvent(r -> r.deployTaskStart(globalModVersion));
	}

	public void deployTaskFinish(long globalModVersion){
		recordCompoundRecorderEvent(r -> r.deployTaskFinish(globalModVersion));
	}

	//---------------------
	// ResourceManager events
	//---------------------
	public void createTaskManagerContextStart(String taskManagerId){
		recordCompoundRecorderEvent(r -> r.createTaskManagerContextStart(taskManagerId));
	}

	public void createTaskManagerContextFinish(String taskManagerId){
		recordCompoundRecorderEvent(r -> r.createTaskManagerContextFinish(taskManagerId));
	}

	public void startContainerStart(String taskManagerId){
		recordCompoundRecorderEvent(r -> r.startContainerStart(taskManagerId));
	}

	public void startContainerFinish(String taskManagerId){
		recordCompoundRecorderEvent(r -> r.startContainerFinish(taskManagerId));
	}

	private void recordCompoundRecorderEvent(
		Consumer<AbstractEventRecorder> consumer) {
		for (AbstractEventRecorder abstractEventRecorder : recorderList) {
			if (abstractEventRecorder instanceof CompoundRecorder) {
				continue;
			}
			consumer.accept(abstractEventRecorder);
		}
	}
}
