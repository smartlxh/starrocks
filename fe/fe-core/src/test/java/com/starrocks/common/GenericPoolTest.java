// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.InternalServiceVersion;
import com.starrocks.thrift.TAgentPublishRequest;
import com.starrocks.thrift.TAgentResult;
import com.starrocks.thrift.TAgentTaskRequest;
import com.starrocks.thrift.TCancelPlanFragmentParams;
import com.starrocks.thrift.TCancelPlanFragmentResult;
import com.starrocks.thrift.TDeleteEtlFilesRequest;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TExecPlanFragmentResult;
import com.starrocks.thrift.TExportStatusResult;
import com.starrocks.thrift.TExportTaskRequest;
import com.starrocks.thrift.TFetchDataParams;
import com.starrocks.thrift.TFetchDataResult;
import com.starrocks.thrift.TGetTabletsInfoRequest;
import com.starrocks.thrift.TGetTabletsInfoResult;
import com.starrocks.thrift.TMiniLoadEtlStatusRequest;
import com.starrocks.thrift.TMiniLoadEtlStatusResult;
import com.starrocks.thrift.TMiniLoadEtlTaskRequest;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanCloseParams;
import com.starrocks.thrift.TScanCloseResult;
import com.starrocks.thrift.TScanNextBatchParams;
import com.starrocks.thrift.TScanOpenParams;
import com.starrocks.thrift.TScanOpenResult;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStreamLoadChannel;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.thrift.TTransmitDataParams;
import com.starrocks.thrift.TTransmitDataResult;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GenericPoolTest {
    static ThriftConnectionPool<BackendService.Client> backendService;
    static ThriftServer service;
    static String ip = "127.0.0.1";
    static int port;

    static {
        port = UtFrameUtils.findValidPort();
    }

    static void close() {
        if (service != null) {
            service.stop();
        }
    }

    @BeforeAll
    public static void beforeClass() throws IOException {
        try {
            GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
            config.setLifo(true); // set Last In First Out strategy
            config.setMaxIdlePerKey(2); // (default 8)
            config.setMinIdlePerKey(0); // (default 0)
            config.setMaxTotalPerKey(2); // (default 8)
            config.setMaxTotal(3); // (default -1)
            config.setMaxWaitMillis(500);
            // new ClientPool
            backendService = new ThriftConnectionPool("BackendService", config, 0);
            // new ThriftService
            TProcessor tprocessor = new BackendService.Processor<BackendService.Iface>(
                    new InternalProcessor());
            service = new ThriftServer(port, tprocessor);
            service.start();
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
            close();
        }
    }

    @AfterAll
    public static void afterClass() throws IOException {
        close();
    }

    private static class InternalProcessor implements BackendService.Iface {
        public InternalProcessor() {
            //
        }

        @Override
        public TExecPlanFragmentResult exec_plan_fragment(TExecPlanFragmentParams params) {
            return new TExecPlanFragmentResult();
        }

        @Override
        public TCancelPlanFragmentResult cancel_plan_fragment(TCancelPlanFragmentParams params) {
            return new TCancelPlanFragmentResult();
        }

        @Override
        public TTransmitDataResult transmit_data(TTransmitDataParams params) {
            return new TTransmitDataResult();
        }

        @Override
        public TFetchDataResult fetch_data(TFetchDataParams params) {
            TFetchDataResult result = new TFetchDataResult();
            result.setPacket_num(123);
            result.setResult_batch(new TResultBatch(new ArrayList<ByteBuffer>(), false, 0));
            result.setEos(true);
            return result;
        }

        @Override
        public TAgentResult submit_tasks(List<TAgentTaskRequest> tasks) throws TException {
            return null;
        }

        @Override
        public TAgentResult release_snapshot(String snapshotPath) throws TException {
            return null;
        }

        @Override
        public TAgentResult publish_cluster_state(TAgentPublishRequest request) throws TException {
            return null;
        }

        @Override
        public TAgentResult submit_etl_task(TMiniLoadEtlTaskRequest request) throws TException {
            return null;
        }

        @Override
        public TMiniLoadEtlStatusResult get_etl_status(TMiniLoadEtlStatusRequest request) throws TException {
            return null;
        }

        @Override
        public TAgentResult delete_etl_files(TDeleteEtlFilesRequest request) throws TException {
            return null;
        }

        @Override
        public TAgentResult make_snapshot(TSnapshotRequest snapshotRequest) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TStatus submit_export_task(TExportTaskRequest request) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TExportStatusResult get_export_status(TUniqueId taskId) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TStatus erase_export_task(TUniqueId taskId) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TTabletStatResult get_tablet_stat() throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TGetTabletsInfoResult get_tablets_info(TGetTabletsInfoRequest request) throws TException {
            return null;
        }

        @Override
        public TStatus submit_routine_load_task(List<TRoutineLoadTask> tasks) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TStatus finish_stream_load_channel(TStreamLoadChannel streamLoadChannel) throws TException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TScanOpenResult open_scanner(TScanOpenParams params) throws TException {
            return null;
        }

        @Override
        public TScanBatchResult get_next(TScanNextBatchParams params) throws TException {
            return null;
        }

        @Override
        public TScanCloseResult close_scanner(TScanCloseParams params) throws TException {
            return null;
        }
    }

    @Test
    public void testNormal() throws Exception {
        TNetworkAddress address = new TNetworkAddress(ip, port);
        BackendService.Client object = backendService.borrowObject(address);

        TFetchDataResult result = object.fetch_data(new TFetchDataParams(
                InternalServiceVersion.V1, new TUniqueId()));
        Assertions.assertEquals(result.getPacket_num(), 123);

        backendService.returnObject(address, object);
    }

    @Test
    public void testSetMaxPerKey() throws Exception {
        TNetworkAddress address = new TNetworkAddress(ip, port);
        BackendService.Client object1;
        BackendService.Client object2;
        BackendService.Client object3;

        // first success
        object1 = backendService.borrowObject(address);

        // second success
        object2 = backendService.borrowObject(address);

        // third fail, because the max connection is 2
        boolean flag = false;
        try {
            object3 = backendService.borrowObject(address);
        } catch (java.util.NoSuchElementException e) {
            flag = true;
            // pass
        } catch (Exception e) {
            Assertions.fail();
        }
        Assertions.assertTrue(flag);

        // fouth success, beacuse we drop the object1
        backendService.returnObject(address, object1);
        object3 = null;
        object3 = backendService.borrowObject(address);
        Assertions.assertTrue(object3 != null);

        backendService.returnObject(address, object2);
        backendService.returnObject(address, object3);
    }

    @Test
    public void testException() throws Exception {
        TNetworkAddress address = new TNetworkAddress(ip, port);
        BackendService.Client object;
        // borrow null
        boolean flag = false;
        try {
            object = backendService.borrowObject(null);
        } catch (NullPointerException e) {
            flag = true;
        }
        Assertions.assertTrue(flag);
        flag = false;
        // return twice
        object = backendService.borrowObject(address);
        backendService.returnObject(address, object);
        try {
            backendService.returnObject(address, object);
        } catch (java.lang.IllegalStateException e) {
            flag = true;
        }
        Assertions.assertTrue(flag);
    }
}
