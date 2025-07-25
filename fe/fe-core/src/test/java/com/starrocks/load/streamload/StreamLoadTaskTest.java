// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.load.streamload;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.WarehouseManager;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.warehouse.WarehouseIdleChecker;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static com.starrocks.common.ErrorCode.ERR_NO_PARTITIONS_HAVE_DATA_LOAD;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamLoadTaskTest {

    @Mocked
    private DefaultCoordinator coord;

    private StreamLoadTask streamLoadTask;

    @BeforeEach
    public void setUp() {
        long id = 123L;
        String label = "label_abc";
        long timeoutMs = 10000L;
        long createTimeMs = System.currentTimeMillis();
        boolean isRoutineLoad = false;
        streamLoadTask =
                new StreamLoadTask(id, new Database(), new OlapTable(), label, "", "", timeoutMs, createTimeMs, isRoutineLoad,
                        WarehouseManager.DEFAULT_RESOURCE);
    }

    @Test
    public void testAfterCommitted() throws StarRocksException {
        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = false;
            }
        };
        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assertions.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        streamLoadTask.afterCommitted(txnState, txnOperated);
        Assertions.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testAfterAborted() throws StarRocksException {
        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = false;
            }
        };
        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;

        TUniqueId labelId = new TUniqueId(2, 3);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assertions.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        long ts = System.currentTimeMillis();
        streamLoadTask.afterAborted(txnState, txnOperated, "");
        Assertions.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
        Assertions.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(streamLoadTask.getCurrentWarehouseId()));
    }

    @Test
    public void testAfterVisible() {
        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        long ts = System.currentTimeMillis();
        streamLoadTask.afterVisible(txnState, txnOperated);
        Assertions.assertTrue(ts <= WarehouseIdleChecker.getLastFinishedJobTime(streamLoadTask.getCurrentWarehouseId()));
    }

    @Test
    public void testNoPartitionsHaveDataLoad() {
        Map<String, String> loadCounters = Maps.newHashMap();
        loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "0");
        loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "0");
        loadCounters.put(LoadJob.UNSELECTED_ROWS, "0");
        loadCounters.put(LoadJob.LOADED_BYTES, "0");

        streamLoadTask.setCoordinator(coord);
        new Expectations() {
            {
                coord.join(anyInt);
                result = true;
                coord.getLoadCounters();
                returns(null, loadCounters);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, ERR_NO_PARTITIONS_HAVE_DATA_LOAD.formatErrorMsg(),
                () -> Deencapsulation.invoke(streamLoadTask, "unprotectedWaitCoordFinish"));
    }

    @Test
    public void testSetLoadStateWithManualLoadTxnCommitAttachment() {
        ManualLoadTxnCommitAttachment attachment = mock(ManualLoadTxnCommitAttachment.class);
        when(attachment.getLoadedRows()).thenReturn(100L);
        when(attachment.getFilteredRows()).thenReturn(10L);
        when(attachment.getUnselectedRows()).thenReturn(5L);
        when(attachment.getLoadedBytes()).thenReturn(1000L);
        when(attachment.getErrorLogUrl()).thenReturn("http://error.log");
        when(attachment.getBeginTxnTime()).thenReturn(100L);
        when(attachment.getReceiveDataTime()).thenReturn(200L);
        when(attachment.getPlanTime()).thenReturn(300L);

        streamLoadTask.setLoadState(attachment, "Error message");

        TLoadInfo loadInfo = streamLoadTask.toThrift();

        Assertions.assertEquals(100L, loadInfo.getNum_sink_rows());
        Assertions.assertEquals(10L, loadInfo.getNum_filtered_rows());
        Assertions.assertEquals(5L, loadInfo.getNum_unselected_rows());
        Assertions.assertEquals(1000L, loadInfo.getNum_scan_bytes());
        Assertions.assertEquals("http://error.log", loadInfo.getUrl());
        Assertions.assertEquals("Error message", loadInfo.getError_msg());
    }

    @Test
    public void testSetLoadStateWithRLTaskTxnCommitAttachment() {
        RLTaskTxnCommitAttachment attachment = mock(RLTaskTxnCommitAttachment.class);
        when(attachment.getLoadedRows()).thenReturn(200L);
        when(attachment.getFilteredRows()).thenReturn(20L);
        when(attachment.getUnselectedRows()).thenReturn(10L);
        when(attachment.getLoadedBytes()).thenReturn(2000L);
        when(attachment.getErrorLogUrl()).thenReturn("http://error.log.rl");

        streamLoadTask.setLoadState(attachment, "Another error message");

        TLoadInfo loadInfo = streamLoadTask.toThrift();

        Assertions.assertEquals(200L, loadInfo.getNum_sink_rows());
        Assertions.assertEquals(20L, loadInfo.getNum_filtered_rows());
        Assertions.assertEquals(10L, loadInfo.getNum_unselected_rows());
        Assertions.assertEquals("http://error.log.rl", loadInfo.getUrl());
        Assertions.assertEquals("Another error message", loadInfo.getError_msg());
    }

    @Test
    public void testBuildProfile() throws StarRocksException {
        streamLoadTask.setCoordinator(coord);
        streamLoadTask.setIsSyncStreamLoad(true);
        new Expectations() {
            {
                coord.isProfileAlreadyReported();
                result = true;
                coord.getQueryProfile();
                result = null;
            }
        };
        TUniqueId labelId = new TUniqueId(4, 5);
        streamLoadTask.setTUniqueId(labelId);
        QeProcessorImpl.INSTANCE.registerQuery(streamLoadTask.getTUniqueId(), coord);
        Assertions.assertEquals(1, QeProcessorImpl.INSTANCE.getCoordinatorCount());

        TransactionState txnState = new TransactionState();
        boolean txnOperated = true;
        streamLoadTask.afterCommitted(txnState, txnOperated);
        Assertions.assertEquals(0, QeProcessorImpl.INSTANCE.getCoordinatorCount());
    }

    @Test
    public void testDuplicateBeginTxn() throws StarRocksException {
        TransactionResult resp = new TransactionResult();
        TUniqueId requestId = new TUniqueId(100056, 560001);
        StreamLoadTask streamLoadTask1 = Mockito.spy(new StreamLoadTask(0, new Database(), new OlapTable(),
                                                                        "", "", "", 10, 10, false,
                                                                        WarehouseManager.DEFAULT_RESOURCE));
        TransactionState.TxnCoordinator coordinator =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "192.168.1.2");
        doThrow(new DuplicatedRequestException("Duplicate request", 0L, ""))
                .when(streamLoadTask1).unprotectedBeginTxn(same(requestId), same(coordinator));
        streamLoadTask1.beginTxn(0, 1, requestId, coordinator, resp);
        Assertions.assertTrue(resp.stateOK());
        streamLoadTask1.beginTxn(0, 1, requestId, coordinator, resp);
        Assertions.assertTrue(resp.stateOK());
    }
}
