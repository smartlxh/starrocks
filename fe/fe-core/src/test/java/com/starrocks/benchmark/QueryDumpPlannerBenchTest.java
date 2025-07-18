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

package com.starrocks.benchmark;

import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.plan.ReplayFromDumpTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Benchmark planner performance for specific query dump
 */
public class QueryDumpPlannerBenchTest extends ReplayFromDumpTestBase {
    private static String sql;

    // NOTE: ignore the test in CI
    // @BeforeClass
    public static void beforeClass() throws Exception {
        ReplayFromDumpTestBase.beforeClass();
        String dump = getDumpInfoFromFile("query_dump/materialized-view/mv_join_rewrite");
        QueryDumpInfo queryDumpInfo = getDumpInfoFromJson(dump);
        sql = UtFrameUtils.setUpTestDump(connectContext, queryDumpInfo);
    }

    // NOTE: ignore the test in CI
    // @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.tearDownTestDump();
        ReplayFromDumpTestBase.afterClass();
    }

    /**
     * round: 0.01 [+- 0.00], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 82, GC.time: 0.14,
     * time.total: 14.64, time.warmup: 0.82, time.bench: 13.82
     */
    @Test
    @Disabled
    //@BenchmarkOptions(concurrency = 1, warmupRounds = 10, benchmarkRounds = 1000)
    public void benchDump() throws Exception {
        connectContext.setThreadLocalInfo();
        UtFrameUtils.replaySql(connectContext, sql);
    }
}
