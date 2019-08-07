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

import {ChangeDetectorRef, Component, OnInit} from '@angular/core';
import {JobManagerService} from "services";
import {Resources, SrResponse} from "../../../interfaces/sr";

@Component({
  selector: 'flink-job-manager-sr',
  templateUrl: './job-manager-sr.component.html',
  styleUrls: ['./job-manager-sr.component.less'],
})
export class JobManagerSrComponent implements OnInit {
  srData: SrResponse;
  adjustHistory: {time: string, memoryMB: bigint, vcores: bigint}[] = [];
  containersStats: {memoryMB: bigint, vcores: bigint, count: bigint}[] = [];
  initialResources: Resources;
  config = new Map();

  constructor(private jobManagerService: JobManagerService, private cdr: ChangeDetectorRef) { }

  ngOnInit() {
    this.jobManagerService.loadSr().subscribe(data => {
      this.srData = data;
      this.adjustHistory = data.adjustHistory;
      this.containersStats = data.containersStats;
      this.initialResources = data.initialResources;
      this.config = data.config;
      this.cdr.markForCheck();
    });
  }
}
