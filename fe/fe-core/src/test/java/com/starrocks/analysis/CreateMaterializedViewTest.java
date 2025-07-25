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

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.mv.MVPlanValidationResult;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.MVTestUtils.waitingRollupJobV2Finish;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateMaterializedViewTest extends MVTestBase {
    private static final Logger LOG = LogManager.getLogger(CreateMaterializedViewTest.class);


    public String name;

    @TempDir
    public static File temp;

    private static ConnectContext connectContext;
    private static Database testDb;
    private static GlobalStateMgr currentState;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.doInit(newFolder(temp, "junit").toURI().toString());

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        Config.default_mv_refresh_immediate = true;

        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.TBL1 \n" +
                        "(\n" +
                        "    K1 date,\n" +
                        "    K2 int,\n" +
                        "    V1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(K1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(K2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `aggregate_table_with_null` (\n" +
                        "`k1` date,\n" +
                        "`v2` datetime MAX,\n" +
                        "`v3` char(20) MIN,\n" +
                        "`v4` bigint SUM,\n" +
                        "`v8` bigint SUM,\n" +
                        "`v5` HLL HLL_UNION,\n" +
                        "`v6` BITMAP BITMAP_UNION,\n" +
                        "`v7` PERCENTILE PERCENTILE_UNION\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withView("CREATE VIEW v1 AS SELECT * FROM aggregate_table_with_null;")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('10'),\n" +
                        "    PARTITION p2 values less than('20')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl4\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    k3 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2,k3)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('20','30'),\n" +
                        "    PARTITION p2 values less than('40','50')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `c_1_0` decimal128(30, 4) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_1` boolean NOT NULL COMMENT \"\",\n" +
                        "  `c_1_2` date NULL COMMENT \"\",\n" +
                        "  `c_1_3` date NOT NULL COMMENT \"\",\n" +
                        "  `c_1_4` double NULL COMMENT \"\",\n" +
                        "  `c_1_5` double NULL COMMENT \"\",\n" +
                        "  `c_1_6` datetime NULL COMMENT \"\",\n" +
                        "  `c_1_7` ARRAY<int(11)> NULL COMMENT \"\",\n" +
                        "  `c_1_8` smallint(6) NULL COMMENT \"\",\n" +
                        "  `c_1_9` bigint(20) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_10` varchar(31) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_11` decimal128(22, 18) NULL COMMENT \"\",\n" +
                        "  `c_1_12` boolean NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`c_1_3`)\n" +
                        "(PARTITION p20000101 VALUES [('2000-01-01'), ('2010-12-31')),\n" +
                        "PARTITION p20101231 VALUES [('2010-12-31'), ('2021-12-30')),\n" +
                        "PARTITION p20211230 VALUES [('2021-12-30'), ('2032-12-29')))\n" +
                        "DISTRIBUTED BY HASH(`c_1_3`, `c_1_2`, `c_1_0`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");")
                .withTable("CREATE EXTERNAL TABLE mysql_external_table\n" +
                        "(\n" +
                        "    k1 DATE,\n" +
                        "    k2 INT,\n" +
                        "    k3 SMALLINT,\n" +
                        "    k4 VARCHAR(2048),\n" +
                        "    k5 DATETIME\n" +
                        ")\n" +
                        "ENGINE=mysql\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    \"host\" = \"127.0.0.1\",\n" +
                        "    \"port\" = \"3306\",\n" +
                        "    \"user\" = \"mysql_user\",\n" +
                        "    \"password\" = \"mysql_passwd\",\n" +
                        "    \"database\" = \"mysql_db_test\",\n" +
                        "    \"table\" = \"mysql_table_test\"\n" +
                        ");")
                .withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE test2.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl5\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    k3 int,\n" +
                        "    v1 int,\n" +
                        "    v2 int\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl_for_count\n" +
                        "(\n" +
                        "   c_0_0 BIGINT NULL ,\n" +
                        "   c_0_1 DATE NOT NULL ,\n" +
                        "   c_0_2 DECIMAL(37, 5)  NOT NULL,\n" +
                        "   c_0_3 INT MAX NOT NULL ,\n" +
                        "   c_0_4 DATE REPLACE_IF_NOT_NULL NOT NULL ,\n" +
                        "   c_0_5 PERCENTILE PERCENTILE_UNION NOT NULL\n" +
                        ")\n" +
                        "AGGREGATE KEY (c_0_0,c_0_1,c_0_2)\n" +
                        "PARTITION BY RANGE(c_0_1)\n" +
                        "(\n" +
                        "   START (\"2010-01-01\") END (\"2021-12-31\") EVERY (INTERVAL 219 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (c_0_2,c_0_1) BUCKETS 3\n" +
                        "properties('replication_num'='1');")
                .withTable("CREATE TABLE test.mocked_cloud_table\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2020-01-01'),('2020-02-01')),\n" +
                        "    PARTITION p2 values [('2020-02-01'),('2020-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .useDatabase("test");
        starRocksAssert.withView("create view test.view_to_tbl1 as select * from test.tbl1;");
        currentState = GlobalStateMgr.getCurrentState();
        testDb = currentState.getLocalMetastore().getDb("test");

        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void afterClass() throws Exception {
    }

    private static void dropMv(String mvName) throws Exception {
        String sql = "drop materialized view " + mvName;
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
        stmtExecutor.execute();
    }

    private List<TaskRunStatus> waitingTaskFinish() {
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        List<TaskRunStatus> taskRuns = taskManager.getMatchedTaskRunStatus(null);
        int retryCount = 0, maxRetry = 50;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(200L);
            Constants.TaskRunState state = taskRuns.get(0).getState();
            if (state.isFinishState()) {
                break;
            }
            retryCount++;
            LOG.info("waiting for TaskRunState retryCount:" + retryCount);
        }
        return taskRuns;
    }

    // ========== full test ==========

    @Test
    public void testFullCreate() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) {
            }
        };

        String sql = String.format(
                "create materialized view mv1\n" +
                        "partition by date_trunc('month',k1)\n" +
                        "distributed by hash(s2) buckets 10\n" +
                        "refresh async START('%s') EVERY(INTERVAL 3 minute)\n" +
                        "PROPERTIES (\n\"replication_num\" = \"1\"\n)\n" +
                        "as select tb1.k1, k2 s2 from tbl1 tb1;",
                LocalDateTime.now().plusSeconds(3).format(DateUtils.DATE_TIME_FORMATTER)
        );

        MaterializedView materializedView = getMaterializedViewChecked(sql);
        validatePartitionInfo(materializedView);
        validateBaseTable(materializedView);
        validateMaterializedViewProperties(materializedView);
        testFullCreateSync(materializedView, getBaseTable(materializedView));
    }

    private void validatePartitionInfo(MaterializedView materializedView) {
        PartitionInfo partitionInfo = materializedView.getPartitionInfo();
        Assertions.assertEquals(1, partitionInfo.getPartitionColumnsSize());
        Assertions.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);

        Expr partitionExpr = ((ExpressionRangePartitionInfo) partitionInfo)
                .getPartitionExprs(materializedView.getIdToColumn()).get(0);
        Assertions.assertTrue(partitionExpr instanceof FunctionCallExpr);
        Assertions.assertEquals("date_trunc", ((FunctionCallExpr) partitionExpr).getFnName().getFunction());
        Assertions.assertEquals("k1", ((SlotRef) ((FunctionCallExpr) partitionExpr).getChild(1)).getColumnName());
    }

    private void validateBaseTable(MaterializedView materializedView) {
        BaseTableInfo baseTableInfo = materializedView.getBaseTableInfos().get(0);
        Table baseTable = getBaseTable(materializedView);
        Assertions.assertNotNull(baseTable);
        Assertions.assertEquals(baseTableInfo.getTableId(), baseTable.getId());
        Assertions.assertEquals(1, baseTable.getRelatedMaterializedViews().size());
        Assertions.assertNotNull(baseTable.getColumn("k1"));
    }

    private void validateMaterializedViewProperties(MaterializedView materializedView) {
        Assertions.assertEquals("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`\nFROM `test`.`tbl1` AS `tb1`",
                materializedView.getViewDefineSql());
        TableProperty tableProperty = materializedView.getTableProperty();
        Assertions.assertEquals(1, tableProperty.getReplicationNum().shortValue());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, materializedView.getState());
        Assertions.assertEquals(KeysType.DUP_KEYS, materializedView.getKeysType());
        Assertions.assertEquals(Table.TableType.MATERIALIZED_VIEW, materializedView.getType());
        Assertions.assertTrue(materializedView.isActive());
    }

    private Table getBaseTable(MaterializedView materializedView) {
        Expr dateTruncFuncExpr = materializedView.getPartitionRefTableExprs().get(0);
        SlotRef slotRef  = (SlotRef) dateTruncFuncExpr.getChild(1);
        TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
        return GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), baseTableName.getTbl());
    }

    public void testFullCreateSync(MaterializedView materializedView, Table baseTable) throws Exception {
        String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
        List<TaskRunStatus> taskRuns = waitingTaskFinish();
        Assertions.assertEquals(Constants.TaskRunState.SKIPPED, taskRuns.get(0).getState());

        validatePartitionSync(baseTable, materializedView);

        executePartitionChange("ALTER TABLE test.tbl1 ADD PARTITION p3 values less than('2020-04-01');", mvTaskName);
        validatePartitionSync(baseTable, materializedView);

        executePartitionChange("ALTER TABLE test.tbl1 DROP PARTITION p3", mvTaskName);
        validatePartitionSync(baseTable, materializedView);
    }

    private void validatePartitionSync(Table baseTable, MaterializedView materializedView) {
        Assertions.assertEquals(baseTable.getPartitions().size(), materializedView.getPartitions().size());
    }

    private void executePartitionChange(String sql, String mvTaskName) throws Exception {
        StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
        new StmtExecutor(connectContext, statement).execute();
        GlobalStateMgr.getCurrentState().getTaskManager().executeTask(mvTaskName);
        waitingTaskFinish();
    }

    @Test
    public void testCreateAsync() {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 MONTH)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
    }

    @Test
    public void testCreateAsyncMVWithDuplicatedProperty() {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));

        String sql2 = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql2, connectContext));

        String sql3 = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql3, connectContext));
    }

    @Test
    public void testCreateAsyncNormal() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        sql = "create materialized view mv1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "partition by date_trunc('month',k1)\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        sql = "create materialized view mv1\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 DAY)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "partition by date_trunc('month',k1)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateAsyncLowercase() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 day)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateAsyncWithSingleTable() throws Exception {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2)\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        RefreshSchemeClause refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
        Assertions.assertEquals(MaterializedView.RefreshType.MANUAL, refreshSchemeDesc.getType());
    }

    @Test
    public void testCreateSyncWithSingleTable() throws Exception {
        String sql = "create materialized view mv1\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertTrue(statementBase instanceof CreateMaterializedViewStmt);
    }

    @Test
    public void testFullCreateMultiTables() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 minute)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',tb1.k1) s1, tb2.k2 s2 from tbl1 tb1 join tbl2 tb2 on tb1.k2 = tb2.k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Table mv1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "mv1");
            Assertions.assertTrue(mv1 instanceof MaterializedView);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            Assertions.assertEquals(1, partitionInfo.getPartitionColumns(materializedView.getIdToColumn()).size());
            Assertions.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            Expr partitionExpr = expressionRangePartitionInfo.getPartitionExprs(materializedView.getIdToColumn()).get(0);
            Assertions.assertTrue(partitionExpr instanceof SlotRef);
            SlotRef partitionSlotRef = (SlotRef) partitionExpr;
            Assertions.assertEquals("s1", partitionSlotRef.getColumnName());
            List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
            Assertions.assertEquals(2, baseTableInfos.size());
            Expr partitionRefTableExpr = materializedView.getPartitionRefTableExprs().get(0);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionRefTableExpr.collect(SlotRef.class, slotRefs);
            SlotRef slotRef = slotRefs.get(0);
            TableName baseTableName = slotRef.getTblNameWithoutAnalyzed();
            Assertions.assertEquals(baseTableName.getDb(), testDb.getFullName());
            Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), baseTableName.getTbl());
            Assertions.assertNotNull(baseTable);
            Assertions.assertTrue(baseTableInfos.stream().anyMatch(baseTableInfo ->
                    baseTableInfo.getTableId() == baseTable.getId()));
            Assertions.assertTrue(1 <= baseTable.getRelatedMaterializedViews().size());
            Column baseColumn = baseTable.getColumn(slotRef.getColumnName());
            Assertions.assertNotNull(baseColumn);
            Assertions.assertEquals("k1", baseColumn.getName());
            // test sql
            Assertions.assertEquals(
                    "SELECT date_trunc('month', `test`.`tb1`.`k1`) AS `s1`, `test`.`tb2`.`k2` AS `s2`\n" +
                            "FROM `test`.`tbl1` AS `tb1` INNER JOIN `test`.`tbl2` AS `tb2` ON `test`.`tb1`.`k2` = `test`.`tb2`.`k2`",
                    materializedView.getViewDefineSql());
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assertions.assertEquals(1, tableProperty.getReplicationNum().shortValue(), 1);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, materializedView.getState());
            Assertions.assertEquals(KeysType.DUP_KEYS, materializedView.getKeysType());
            Assertions.assertEquals(Table.TableType.MATERIALIZED_VIEW,
                    materializedView.getType()); //TableTypeMATERIALIZED_VIEW
            Assertions.assertEquals(0, materializedView.getRelatedMaterializedViews().size());
            Assertions.assertEquals(2, materializedView.getBaseSchema().size());
            Assertions.assertTrue(materializedView.isActive());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testFullCreateNoPartition() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 minute) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Table mv1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "mv1");
            Assertions.assertTrue(mv1 instanceof MaterializedView);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            PartitionInfo partitionInfo = materializedView.getPartitionInfo();
            Assertions.assertTrue(partitionInfo instanceof SinglePartitionInfo);
            Assertions.assertEquals(1, materializedView.getPartitions().size());
            Partition partition = materializedView.getPartitions().iterator().next();
            Assertions.assertNotNull(partition);
            Assertions.assertEquals("mv1", partition.getName());
            List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
            Assertions.assertEquals(1, baseTableInfos.size());
            Table baseTable = testDb.getTable(baseTableInfos.iterator().next().getTableId());
            Assertions.assertTrue(1 <= baseTable.getRelatedMaterializedViews().size());
            // test sql
            Assertions.assertEquals("SELECT `test`.`tbl1`.`k1`, `test`.`tbl1`.`k2`\nFROM `test`.`tbl1`",
                    materializedView.getViewDefineSql());
            // test property
            TableProperty tableProperty = materializedView.getTableProperty();
            Assertions.assertEquals(1, tableProperty.getReplicationNum().shortValue());
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, materializedView.getState());
            Assertions.assertEquals(KeysType.DUP_KEYS, materializedView.getKeysType());
            Assertions.assertEquals(Table.TableType.MATERIALIZED_VIEW,
                    materializedView.getType()); //TableTypeMATERIALIZED_VIEW
            Assertions.assertEquals(0, materializedView.getRelatedMaterializedViews().size());
            Assertions.assertEquals(2, materializedView.getBaseSchema().size());
            MaterializedView.AsyncRefreshContext asyncRefreshContext =
                    materializedView.getRefreshScheme().getAsyncRefreshContext();
            Assertions.assertTrue(asyncRefreshContext.getStartTime() > 0);
            Assertions.assertEquals("MINUTE", asyncRefreshContext.getTimeUnit());
            Assertions.assertEquals(3, asyncRefreshContext.getStep());
            Assertions.assertTrue(materializedView.isActive());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testCreateWithoutBuckets() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
            }
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 minute) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testPartitionByTableAlias() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1 tb1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionNoDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("No database selected"));
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionHasDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view test.mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from test.tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionNoNeed() {
        String sql = "create materialized view mv1 " +
                "partition by (a+b) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Unsupported expr 'a + b' in PARTITION BY clause"),
                    e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testCreateMVWithExplainQuery() {
        String sql = "create materialized view mv1 " +
                "as explain select k1, v2 from aggregate_table_with_null;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("Creating materialized view does not support explain query", e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testPartitionWithFunctionIn() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Expr partitionByExpr = getMVPartitionByExprChecked(createMaterializedViewStatement);
            Assertions.assertTrue(partitionByExpr instanceof SlotRef);
            SlotRef slotRef = (SlotRef) partitionByExpr;
            Assertions.assertEquals("ss", slotRef.getColumnName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionInUseStr2Date() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Expr partitionByExpr = getMVPartitionByExprChecked(createMaterializedViewStatement);
            Assertions.assertTrue(partitionByExpr instanceof SlotRef);
            SlotRef slotRef = (SlotRef) partitionByExpr;
            Assertions.assertEquals("ss", slotRef.getColumnName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionInUseStr2DateForError() {
        String sql = "create materialized view mv_error " +
                "partition by ss " +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select str2date(d,'%Y%m%d') ss, a, b, c from jdbc0.partitioned_db0.tbl0;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Materialized view partition function str2date check failed"));
        }
    }

    private static Expr getMVPartitionByExprChecked(CreateMaterializedViewStatement stmt) {
        List<Expr> mvPartitionByExprs = stmt.getPartitionByExprs();
        Assertions.assertEquals(1, mvPartitionByExprs.size());
        return mvPartitionByExprs.get(0);
    }

    @Test
    public void testPartitionWithFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Expr partitionByExpr = getMVPartitionByExprChecked(createMaterializedViewStatement);
            Assertions.assertTrue(partitionByExpr instanceof FunctionCallExpr);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionByExpr.collect(SlotRef.class, slotRefs);
            Assertions.assertEquals(partitionByExpr.getChild(1), slotRefs.get(0));
            Assertions.assertEquals("ss", slotRefs.get(0).getColumnName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionUseStr2Date() throws Exception {
        // basic
        {
            String sql = "create materialized view mv1 " +
                    "partition by str2date(d,'%Y%m%d') " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select a, b, c, d from jdbc0.partitioned_db0.tbl1;";
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Expr partitionByExpr = getMVPartitionByExprChecked(createMaterializedViewStatement);
            Assertions.assertTrue(partitionByExpr instanceof FunctionCallExpr);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionByExpr.collect(SlotRef.class, slotRefs);
            Assertions.assertEquals(partitionByExpr.getChild(0), slotRefs.get(0));
            Assertions.assertEquals("d", slotRefs.get(0).getColumnName());
        }

        // slot
        {
            String sql = "create materialized view mv_str2date " +
                    "partition by p " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d,'%Y%m%d') as p,  a, b, c, d from jdbc0.partitioned_db0.tbl1;";
            starRocksAssert.withMaterializedView(sql);
        }

        // rollup
        {
            String sql = "create materialized view mv_date_trunc_str2date " +
                    "partition by date_trunc('month', p) " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d,'%Y%m%d') as p,  a, b, c, d from jdbc0.partitioned_db0.tbl1;";
            starRocksAssert.withMaterializedView(sql);
        }
    }

    @Test
    public void testPartitionWithFunctionNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Expr partitionByExpr = getMVPartitionByExprChecked(createMaterializedViewStatement);
            Assertions.assertTrue(partitionByExpr instanceof FunctionCallExpr);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionByExpr.collect(SlotRef.class, slotRefs);
            Assertions.assertEquals(partitionByExpr.getChild(1), slotRefs.get(0));
            Assertions.assertEquals("k1", slotRefs.get(0).getColumnName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithoutFunction() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Expr partitionByExpr = getMVPartitionByExprChecked(createMaterializedViewStatement);
            Assertions.assertTrue(partitionByExpr instanceof SlotRef);
            List<SlotRef> slotRefs = Lists.newArrayList();
            partitionByExpr.collect(SlotRef.class, slotRefs);
            Assertions.assertEquals(partitionByExpr, slotRefs.get(0));
            Assertions.assertEquals("k1", slotRefs.get(0).getColumnName());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunction() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',date_trunc('month',ss)) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Unsupported expr 'date_trunc('month', " +
                    "date_trunc('month', ss))' in PARTITION BY clause"), e.getMessage());
        }
    }

    @Test
    public void testPartitionWithFunctionIncludeFunctionInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',ss) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 1, column 42 to line 1, column 63. " +
                            "Detail message: Materialized view partition function date_trunc must related with column.",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionColumnNoBaseTablePartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Materialized view partition column " +
                    "in partition exp must be base table partition column.", e.getMessage());
        }
    }

    @Test
    public void testPartitionColumnBaseTableHasMultiPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s2 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl4;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Materialized view related base table " +
                    "partition columns only supports single column.", e.getMessage());
        }
    }

    @Test
    public void testBaseTableNoPartitionColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 s1, k2 s2 from tbl3;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Materialized view partition column" +
                    " in partition exp must be base table partition column.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumn() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement statementBase =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<BaseTableInfo> baseTableInfos = statementBase.getBaseTableInfos();
            Assertions.assertEquals(1, baseTableInfos.size());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnMixAlias1() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnMixAlias2() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByColumnNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by s8 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k2,sqrt(tbl1.k1) s1 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Materialized view partition exp " +
                    "column:s8 is not found in query statement.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByFunctionNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',s8) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Materialized view partition exp " +
                    "column:s8 is not found in query statement.", e.getMessage());
        }
    }

    @Test
    public void testPartitionByFunctionColumnNoExists() {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',tb2.k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select tb1.k1, tb2.k2 s2 from tbl1 tb1 join tbl2 tb2 on tb1.k2 = tb2.k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 2, column 13 to line 2, column 38. " +
                            "Detail message: Materialized view partition exp: `tb2`.`k1` must related to column.",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc(tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 29. " +
                    "Detail message: No matching function with signature: date_trunc(date).", e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('%y%m',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error at line 3, column 29. " +
                            "Detail message: date_trunc function can't support argument other than year|quarter|month|week|day.",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoCorrParams1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 32. " +
                    "Detail message: Materialized view partition function date_trunc check failed: " +
                    "date_trunc('month', `k2`).", e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionUseWeek1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('week',k2) ss, k2 from tbl2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 31. " +
                    "Detail message: Materialized view partition function date_trunc check failed: " +
                    "date_trunc('week', `k2`).", e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionUseWeek2() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('week',k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByAllowedFunctionUseWeek3() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('week', k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testPartitionByNoAllowedFunction() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k2, sqrt(tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 16 to line 3, column 28. " +
                            "Detail message: Materialized view partition function sqrt is not support: sqrt(`k1`).",
                    e.getMessage());
        }
    }

    @Test
    public void testPartitionByNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('month',k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Partition exp date_trunc('month', k1) must be alias of select item", e.getMessage());
        }
    }

    // ========== distributed test  ==========
    @Test
    public void testDistributeKeyIsNotKey() {
        String sql = "create materialized view mv1 " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";

        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testDistributeByIsNull1() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testDistributeByIsNull2() {
        connectContext.getSessionVariable().setAllowDefaultPartition(true);
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            connectContext.getSessionVariable().setAllowDefaultPartition(false);
        }
    }

    // ========== refresh test ==========
    @Test
    public void testRefreshAsyncOnlyEvery() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async EVERY(INTERVAL 2 MINUTE)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeClause refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            Assertions.assertEquals(MaterializedView.RefreshType.ASYNC, refreshSchemeDesc.getType());
            Assertions.assertNotNull(asyncRefreshSchemeDesc.getStartTime());
            Assertions.assertEquals(2, ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue());
            Assertions.assertEquals("MINUTE",
                    asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            dropMv("mv1");
        }
    }

    @Test
    public void testRefreshAsyncStartBeforeCurr() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2016-12-31') EVERY(INTERVAL 1 HOUR)" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeClause refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            Assertions.assertEquals(MaterializedView.RefreshType.ASYNC, refreshSchemeDesc.getType());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshManual() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh manual " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeClause refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            Assertions.assertEquals(MaterializedView.RefreshType.MANUAL, refreshSchemeDesc.getType());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testNoRefresh() {
        String sql = "create materialized view mv1 " +
                "as select tbl1.k1 ss, k2 from tbl1 group by k1, k2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.assertTrue(statementBase instanceof CreateMaterializedViewStmt);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testNoRefreshNoSelectStmt() {
        String sql = "create materialized view mv1 " +
                "as select t1.k1 ss, t1.k2 from tbl1 t1 union select k1, k2 from tbl1 group by tbl1.k1, tbl1.k2;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Materialized view query statement only supports a single query blocks"));
        }
    }

    // ========== as test ==========
    @Test
    public void testSetOperation() throws Exception {
        for (String setOp : Arrays.asList("UNION", "UNION ALL", "INTERSECT", "EXCEPT")) {
            String sql = String.format("create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(k2) buckets 10 " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")" +
                    "as select t1.k1 ss, t1.k2 from tbl1 t1 %s select k1, k2 from tbl2 t2;", setOp);
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        // All select list must be validated
        Assertions.assertThrows(AnalysisException.class, () -> {
            String sql1 = "create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(k2) buckets 10 " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")" +
                    "as select t1.k1 ss, t1.k2 from tbl1 t1 union select * from tbl2 t2;";
            UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
        }, "hehe");

        Assertions.assertThrows(AnalysisException.class, () -> {
            String sql1 = "create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(k2) buckets 10 " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")" +
                    "as select t1.k1 ss, t1.k2 from tbl1 t1 union select k1, k2 from tbl2 t2 union select * from tbl2 t3";
            UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
        }, "hehe");
    }

    @Test
    public void testAsTableNotInOneDatabase() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select t1.k1 ss, t1.k2 from test2.tbl3 t1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Materialized view do not support table: tbl3 " +
                    "do not exist in database: test", e.getMessage());
        }
    }

    @Test
    public void testMySQLTable() throws Exception {
        String sql1 = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);

        String sql2 = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage()
                    .contains("Materialized view with partition does not support base table type : MYSQL"));
        }
    }

    @Test
    public void testCreateMvFromMv() {
        String sql1 = "create materialized view base_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        String sql2 = "create materialized view mv_from_base_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from base_mv;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvFromMv2() throws Exception {
        String sql1 = "create materialized view base_mv2 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        }

        String sql2 = "create materialized view mv_from_base_mv2 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from base_mv2;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvFromInactiveMv() {
        String sql1 = "create materialized view base_inactive_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }

        MaterializedView baseInactiveMv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "base_inactive_mv"));
        baseInactiveMv.setInactiveAndReason("");

        String sql2 = "create materialized view mv_from_base_inactive_mv " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from base_inactive_mv;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error at line 3, column 24. Detail message: " +
                            "Create/Rebuild materialized view from inactive materialized view: base_inactive_mv.",
                    e.getMessage());
        }
    }

    @Test
    public void testAsHasStar() throws Exception {
        String sql = "create materialized view testAsHasStar " +
                "partition by ss " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 ss, *  from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "testAsHasStar"));
            mv.setInactiveAndReason("");
            List<Column> mvColumns = mv.getFullSchema();

            Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "tbl1");
            List<Column> baseColumns = baseTable.getFullSchema();

            Assertions.assertEquals(mvColumns.size(), baseColumns.size() + 1);
            Assertions.assertEquals("ss", mvColumns.get(0).getName());
            for (int i = 1; i < mvColumns.size(); i++) {
                Assertions.assertEquals(mvColumns.get(i).getName(),
                        baseColumns.get(i - 1).getName());
            }
        } catch (Exception e) {
            Assertions.fail("Select * should be supported in materialized view");
        } finally {
            dropMv("testAsHasStar");
        }
    }

    @Test
    public void testAsHasStarWithSameColumns() throws Exception {
        String sql = "create materialized view testAsHasStar " +
                "partition by ss " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select a.k1 ss, a.*, b.* from tbl1 as a join tbl1 as b on a.k1=b.k1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Duplicate column name 'k1'"));
        } finally {
            dropMv("testAsHasStar");
        }
    }

    @Test
    public void testMVWithSameColumns() throws Exception {
        String sql = "create materialized view testAsHasStar " +
                "partition by ss " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select a.k1 ss, a.k2, b.k2 from tbl1 as a join tbl1 as b on a.k1=b.k1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Duplicate column name 'k2'"));
        } finally {
            dropMv("testAsHasStar");
        }
    }

    @Test
    public void testAsHasStarWithNondeterministicFunction() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(ss) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1 ss, *  from (select *, rand(), current_date() from tbl1) as t;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 38 to line 3, column 43." +
                    " Detail message: Materialized view query statement select item rand()" +
                    " not supported nondeterministic function.", e.getMessage());
        }
    }

    @Test
    public void testAsSelectItemAlias1() throws Exception {
        String sql = "create materialized view testAsSelectItemAlias1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "testAsSelectItemAlias1"));
            mv.setInactiveAndReason("");
            List<Column> mvColumns = mv.getFullSchema();

            Assertions.assertEquals("date_trunc('month', tbl1.k1)", mvColumns.get(0).getName());
            Assertions.assertEquals("k1", mvColumns.get(1).getName());
            Assertions.assertEquals("k2", mvColumns.get(2).getName());

        } catch (Exception e) {
            Assertions.fail("Materialized view query statement select item " +
                    "date_trunc('month', `tbl1`.`k1`) should be supported");
        } finally {
            dropMv("testAsSelectItemAlias1");
        }
    }

    @Test
    public void testAsSelectItemAlias2() throws Exception {
        String sql = "create materialized view testAsSelectItemAlias2 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as " +
                "select date_trunc('month',tbl1.k1), k1, k2 from tbl1 union all " +
                "select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "testAsSelectItemAlias2"));
            mv.setInactiveAndReason("");
            List<Column> mvColumns = mv.getFullSchema();

            Assertions.assertEquals("date_trunc('month', tbl1.k1)", mvColumns.get(0).getName());
            Assertions.assertEquals("k1", mvColumns.get(1).getName());
            Assertions.assertEquals("k2", mvColumns.get(2).getName());

        } finally {
            dropMv("testAsSelectItemAlias2");
        }
    }

    @Test
    // partition by expr is still not supported.
    public void testAsSelectItemAlias3() {
        String sql = "create materialized view testAsSelectItemAlias3 " +
                "partition by date_trunc('month',tbl1.k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Materialized view partition exp: " +
                    "`tbl1`.`k1` must related to column"));
        }
    }

    @Test
    // distribution by expr is still not supported.
    public void testAsSelectItemAlias4() {
        String sql = "create materialized view testAsSelectItemAlias4 " +
                "partition by k1 " +
                "distributed by hash(date_trunc('month',tbl1.k1)) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage()
                    .contains("No viable statement for input 'distributed by hash(date_trunc('."));
        }
    }

    @Test
    public void testAsSelectItemNoAliasWithNondeterministicFunction1() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select rand(), date_trunc('month',tbl1.k1), k1, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 16. " +
                            "Detail message: Materialized view query statement select item rand() not supported " +
                            "nondeterministic function.",
                    e.getMessage());
        }
    }

    @Test
    public void testAsSelectItemHasNonDeterministicFunction1() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select rand() s1, date_trunc('month',tbl1.k1) ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 11 to line 3, column 16. " +
                    "Detail message: Materialized view query statement " +
                    "select item rand() not supported nondeterministic function.", e.getMessage());
        }
    }

    @Test
    public void testAsSelectItemHasNonDeterministicFunction2() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select k2, rand()+rand() s1, date_trunc('month',tbl1.k1) ss from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error from line 3, column 15 to line 3, column 20. " +
                    "Detail message: Materialized view query statement " +
                    "select item rand() not supported nondeterministic function.", e.getMessage());
        }
    }

    // ========== test colocate mv ==========
    @Test
    public void testCreateColocateMvToExitGroup() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.colocateTable\n" +
                "(\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"colocate_group1\"\n" +
                ");");

        new MockUp<OlapTable>() {
            @Mock
            public boolean isEnableColocateMVIndex() throws Exception {
                OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "colocateTable");

                // If the table's colocate group is empty, return false
                if (Strings.isNullOrEmpty(table.getColocateGroup())) {
                    return false;
                }

                // If all indexes except the basic index are all colocate, we can use colocate mv index optimization.
                return table.getIndexIdToMeta().values().stream()
                        .filter(x -> x.getIndexId() != table.getBaseIndexId())
                        .allMatch(MaterializedIndexMeta::isColocateMVIndex);
            }
        };
        String sql = "create materialized view colocateMv\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStmt) statementBase);
            waitingRollupJobV2Finish();
            ColocateTableIndex colocateTableIndex = currentState.getColocateTableIndex();
            String fullGroupName = testDb.getId() + "_" + "colocate_group1";
            long tableId = colocateTableIndex.getTableIdByGroup(fullGroupName);
            Assertions.assertNotEquals(-1, tableId);

            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
            Assertions.assertEquals(1, colocateTableIndex.getAllTableIds(groupId).size());
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "colocateTable");
            Assertions.assertTrue(table.isEnableColocateMVIndex());

            dropMv("colocateMv");
            Assertions.assertTrue(currentState.getColocateTableIndex().isColocateTable(tableId));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            currentState.getColocateTableIndex().clear();
        }
    }

    @Test
    public void testCreateColocateMvWithoutGroup() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.colocateTable2\n" +
                "(\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        ColocateTableIndex colocateTableIndex = currentState.getColocateTableIndex();
        String fullGroupName = testDb.getId() + "_" + "group2";
        long tableId = colocateTableIndex.getTableIdByGroup(fullGroupName);
        Assertions.assertEquals(-1, tableId);

        String sql = "create materialized view colocateMv2\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable2;";

        Assertions.assertThrows(AnalysisException.class, () -> {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStmt) statementBase);
        });

        currentState.getColocateTableIndex().clear();
    }

    @Test
    public void testColocateMvAlterGroup() throws Exception {

        starRocksAssert.withTable("CREATE TABLE test.colocateTable3\n" +
                "(\n" +
                "    k1 int,\n" +
                "    k2 int,\n" +
                "    k3 int\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"group3\"\n" +
                ");");

        new MockUp<OlapTable>() {
            @Mock
            public boolean isEnableColocateMVIndex() throws Exception {
                OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "colocateTable3");

                // If the table's colocate group is empty, return false
                if (Strings.isNullOrEmpty(table.getColocateGroup())) {
                    return false;
                }

                // If all indexes except the basic index are all colocate, we can use colocate mv index optimization.
                return table.getIndexIdToMeta().values().stream()
                        .filter(x -> x.getIndexId() != table.getBaseIndexId())
                        .allMatch(MaterializedIndexMeta::isColocateMVIndex);
            }
        };
        String sql = "create materialized view colocateMv3\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable3;";
        String sql2 = "create materialized view colocateMv4\n" +
                "PROPERTIES (\n" +
                "\"colocate_mv\" = \"true\"\n" +
                ")\n" +
                "as select k1, k2 from colocateTable3;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStmt) statementBase);
            waitingRollupJobV2Finish();
            statementBase = UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
            currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStmt) statementBase);
            waitingRollupJobV2Finish();

            ColocateTableIndex colocateTableIndex = currentState.getColocateTableIndex();
            String fullGroupName = testDb.getId() + "_" + "group3";
            System.out.println(fullGroupName);
            long tableId = colocateTableIndex.getTableIdByGroup(fullGroupName);
            Assertions.assertNotEquals(-1, tableId);

            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "colocateTable3");
            Assertions.assertTrue(table.isEnableColocateMVIndex());

            ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
            Assertions.assertEquals(1, colocateTableIndex.getAllTableIds(groupId).size());

            sql = "alter table colocateTable3 set (\"colocate_with\" = \"groupNew\")";
            statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statementBase);
            stmtExecutor.execute();

            Assertions.assertEquals("groupNew", table.getColocateGroup());
            Assertions.assertTrue(table.isEnableColocateMVIndex());
            Assertions.assertTrue(colocateTableIndex.isColocateTable(tableId));

            sql = "alter table colocateTable3 set (\"colocate_with\" = \"\")";
            statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            stmtExecutor = new StmtExecutor(connectContext, statementBase);
            stmtExecutor.execute();

            Assertions.assertFalse(colocateTableIndex.isColocateTable(tableId));
            Assertions.assertFalse(table.isEnableColocateMVIndex());
            Assertions.assertNotEquals("group1", table.getColocateGroup());

            dropMv("colocateMv4");
            dropMv("colocateMv3");
            Assertions.assertFalse(colocateTableIndex.isColocateTable(tableId));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            currentState.getColocateTableIndex().clear();
        }
    }

    @Test
    public void testRandomColocate() {
        String sql = "create materialized view mv1 " +
                "distributed by random " +
                "refresh async " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n," +
                "'colocate_with' = 'hehe' " +
                ")" +
                "as select k2, date_trunc('month',tbl1.k1) ss from tbl1;";
        Assertions.assertThrows(SemanticException.class, () -> starRocksAssert.withMaterializedView(sql));
    }

    // ========== other test ==========
    @Test
    public void testDisabled() {
        Config.enable_materialized_view = false;
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("The experimental mv is disabled", e.getMessage());
        } finally {
            Config.enable_materialized_view = true;
        }
    }

    @Test
    public void testExists() {
        String sql = "create materialized view tbl1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertEquals("Table 'tbl1' already exists", e.getMessage());
        }
    }

    @Test
    public void testIfNotExists() {
        String sql = "create materialized view if not exists mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testSupportedProperties() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"2122-12-31 23:59:59\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    private void assertParseFailWithException(String sql, String msg) {
        CreateMaterializedViewStatement stmt = null;
        try {
            stmt = (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                    connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(msg));
        }
    }

    private void assertCreateFailWithException(String sql, String msg) {
        CreateMaterializedViewStatement stmt = null;
        try {
            stmt = (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                    connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }

        try {
            currentState.getLocalMetastore().createMaterializedView(stmt);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(msg));
        }
    }

    @Test
    public void testUnSupportedProperties() {
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"short_key\" = \"20\"\n" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        assertCreateFailWithException(sql, "Invalid parameter Analyze materialized properties failed because unknown " +
                "properties");
    }

    @Test
    public void testCreateMVWithSessionProperties1() {
        String sql = "create materialized view mv_with_property1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"session.query_timeout\" = \"10000\"" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";

        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            starRocksAssert.getCtx());
            currentState.getLocalMetastore().createMaterializedView(stmt);
            Table mv1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "mv_with_property1");
            Assertions.assertTrue(mv1 instanceof MaterializedView);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateMVWithSessionProperties2() {
        String sql = "create materialized view mv_with_property2 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"query_timeout\" = \"10000\"" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        assertCreateFailWithException(sql, "Invalid parameter Analyze materialized properties failed because unknown " +
                "properties");
    }

    @Test
    public void testCreateMVWithSessionProperties3() {
        String sql = "create materialized view mv_with_property3 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"session.query_timeout1\" = \"10000\"" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        assertCreateFailWithException(sql, "Unknown system variable 'query_timeout1'");
    }

    @Test
    public void testNoDuplicateKey() {
        String sql = "create materialized view testNoDuplicateKey " +
                "partition by s1 " +
                "distributed by hash(s2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";

        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.getLocalMetastore().createMaterializedView(stmt);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateMvWithImplicitColumnReorder() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_column_reorder refresh async as " +
                "select c_1_3, c_1_4, c_1_0, c_1_1, c_1_2 from t1");
        MaterializedView mv = starRocksAssert.getMv("test", "mv_column_reorder");
        List<String> keys = mv.getKeyColumns().stream().map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(List.of("c_1_0", "c_1_1", "c_1_2", "c_1_3"), keys);
        Assertions.assertEquals(List.of(0, 4, 1, 2, 3), mv.getQueryOutputIndices());
        String ddl = mv.getMaterializedViewDdlStmt(false);
        Assertions.assertTrue(ddl.contains("(`c_1_3`, `c_1_4`, `c_1_0`, `c_1_1`, `c_1_2`)"), ddl);
        Assertions.assertTrue(ddl.contains(" SELECT `test`.`t1`.`c_1_3`, `test`.`t1`.`c_1_4`, " +
                "`test`.`t1`.`c_1_0`, `test`.`t1`.`c_1_1`, `test`.`t1`.`c_1_2`"), ddl);
    }

    @Test
    public void testCreateMvWithSortCols() throws Exception {
        starRocksAssert.dropMaterializedView("mv1");
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`k1`, `s2`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                    UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<String> keyColumns = createMaterializedViewStatement.getMvColumnItems().stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            Assertions.assertEquals(2, createMaterializedViewStatement.getSortKeys().size());
            Assertions.assertEquals(Arrays.asList("k1", "s2"), keyColumns);

            starRocksAssert.withMaterializedView(sql);
            String ddl = starRocksAssert.getMv("test", "mv1")
                    .getMaterializedViewDdlStmt(false);
            Assertions.assertTrue(ddl.contains("(`k1`, `s2`)"), ddl);
            Assertions.assertTrue(ddl.contains("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`"), ddl);
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            Assertions.assertEquals(Lists.newArrayList("k1", "s2"), mv.getTableProperty().getMvSortKeys());
            Assertions.assertEquals("k1,s2",
                    mv.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTY_MV_SORT_KEYS));
            Assertions.assertTrue(ddl.contains("PROPERTIES (\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"storage_medium\" = \"HDD\"\n" +
                    ")"), ddl);
            starRocksAssert.dropMaterializedView("mv1");
        }

        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`s2`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                    UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<String> keyColumns = createMaterializedViewStatement.getMvColumnItems().stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("s2"), keyColumns);

            starRocksAssert.withMaterializedView(sql);
            String ddl = starRocksAssert.getMv("test", "mv1")
                    .getMaterializedViewDdlStmt(false);
            Assertions.assertTrue(ddl.contains("(`k1`, `s2`)"), ddl);
            Assertions.assertTrue(ddl.contains("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`"), ddl);
            starRocksAssert.dropMaterializedView("mv1");
        }
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`k1`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            CreateMaterializedViewStatement createMaterializedViewStatement = (CreateMaterializedViewStatement)
                    UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            List<String> keyColumns = createMaterializedViewStatement.getMvColumnItems().stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList("k1"), keyColumns);

            starRocksAssert.withMaterializedView(sql);
            String ddl = starRocksAssert.getMv("test", "mv1")
                    .getMaterializedViewDdlStmt(false);
            Assertions.assertTrue(ddl.contains("(`k1`, `s2`)"), ddl);
            Assertions.assertTrue(ddl.contains("SELECT `test`.`tb1`.`k1`, `test`.`tb1`.`k2` AS `s2`"), ddl);
            starRocksAssert.dropMaterializedView("mv1");
        }
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`k3`)\n" +
                    "as select tb1.k1, k2 s2 from tbl1 tb1;";
            Assertions.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
        {
            String sql = "create materialized view mv1\n" +
                    "distributed by hash(s2)\n" +
                    "order by (`c_1_7`)\n" +
                    "as select * from t1;";
            Assertions.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
    }

    @Test
    public void testCreateMvWithInvalidSortCols() throws Exception {
        String sql = "create materialized view mv1\n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2)\n" +
                "order by (`s2`, `k1`)\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        List<Column> sortKeys = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(sortKeys.get(0).getName().equals("s2"));
        Assertions.assertTrue(sortKeys.get(1).getName().equals("k1"));
    }

    @Test
    public void testCreateMvWithColocateGroup() throws Exception {
        String groupName = name;
        String sql = "create materialized view mv1 " +
                "partition by ss " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "'colocate_with' = '" + groupName + "'" +
                ") " +
                "as select tbl1.k1 ss, k2 from tbl1;";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        currentState.getLocalMetastore().createMaterializedView((CreateMaterializedViewStatement) statementBase);
        String fullGroupName = testDb.getId() + "_" + groupName;
        long tableId = currentState.getColocateTableIndex().getTableIdByGroup(fullGroupName);
        Assertions.assertTrue(tableId > 0);
    }

    @Test
    public void testCreateMvWithHll() {
        String sql = "CREATE MATERIALIZED VIEW mv_function\n" +
                "AS SELECT k1,MAX(v2),MIN(v3),SUM(v4),HLL_UNION(v5),BITMAP_UNION(v6),PERCENTILE_UNION(v7)\n" +
                "FROM test.aggregate_table_with_null GROUP BY k1\n" +
                "ORDER BY k1 DESC";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvBaseOnView() {
        String sql = "CREATE MATERIALIZED VIEW mv1\n" +
                "AS SELECT k1,v2 FROM test.v1";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals("Getting analyzing error. Detail message: Do not support alter non-OLAP table[v1].",
                    e.getMessage());
        }
    }

    @Test
    public void testAggregateTableWithCount() {
        String sql = "CREATE MATERIALIZED VIEW v0 AS SELECT t0_57.c_0_1," +
                " COUNT(t0_57.c_0_0) , MAX(t0_57.c_0_2) , MAX(t0_57.c_0_3) , MIN(t0_57.c_0_4)" +
                " FROM tbl_for_count AS t0_57 GROUP BY t0_57.c_0_1 ORDER BY t0_57.c_0_1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Getting analyzing error. Detail message: Aggregate type table do not " +
                            "support count function in materialized view."));
        }
    }

    @Test
    public void testNoExistDb() {
        String sql = "create materialized view unexisted_db1.mv1\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        assertParseFailWithException(sql, "Can not find database:unexisted_db1 in default_catalog.");
    }

    @Test
    public void testMvNameInvalid() {
        String sql = "create materialized view mvklajksdjksjkjfksdlkfgkllksdjkgjsdjfjklsdjkfgjkldfkljgljkljklgja\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.getLocalMetastore().createMaterializedView(stmt);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testMvName1() {
        String sql = "create materialized view 22mv\n" +
                "partition by s1\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "as select date_trunc('month',k1) s1, k2 s2 from tbl1;";
        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.getLocalMetastore().createMaterializedView(stmt);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testPartitionAndDistributionByColumnNameIgnoreCase() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, tbl1.k2 from tbl1;";
        try {
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            connectContext);
            currentState.getLocalMetastore().createMaterializedView(stmt);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testDuplicateColumn() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, K1 from tbl1;";
        assertParseFailWithException(sql, "Getting analyzing error. Detail message: Duplicate column name 'K1'.");
    }

    @Test
    public void testNoBaseTable() {
        String sql = "create materialized view mv1 " +
                "partition by K1 " +
                "distributed by hash(K2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select 1 as k1, 2 as k2";
        assertParseFailWithException(sql, "Getting analyzing error. Detail message: Can not find base " +
                "table in query statement.");
    }

    @Test
    public void testUseCte() throws Exception {
        String sql = "create materialized view mv1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH ASYNC\n" +
                "AS with tbl as\n" +
                "(select * from tbl1)\n" +
                "SELECT k1,k2\n" +
                "FROM tbl;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        sql = "create materialized view mv1\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH ASYNC AS " +
                "WITH cte1 AS (select k1, k2 from tbl1),\n" +
                "     cte2 AS (select count(*) cnt from tbl1)\n" +
                "SELECT cte1.k1, cte2.cnt\n" +
                "FROM cte1, cte2;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testUseSubQuery() throws Exception {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from (select * from tbl1) tbl";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testUseSubQueryWithPartition() throws Exception {
        String sql1 = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from (select * from tbl1) tbl";

        String sql2 = "create materialized view mv2 " +
                "partition by kk " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select date_trunc('day', k1) as kk, k2 from (select * from tbl1) tbl";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
            UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testJoinWithPartition() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by date_trunc('day', k1) " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select tb1.kk as k1, tb2.k2 as k2 from (select k1 as kk, k2 from tbl1) tb1 join (select * from tbl2) tb2 on tb1.kk = tb2.k1";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testPartitionByNotFirstColumn() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_with_partition_by_not_first_column" +
                " partition by k1" +
                " distributed by hash(k3) buckets 10" +
                " as select k3, k1, sum(v1) as total from tbl5 group by k3, k1");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "mv_with_partition_by_not_first_column");
        Assertions.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assertions.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        List<Expr> partitionExpr = expressionRangePartitionInfo.getPartitionExprs(table.getIdToColumn());
        Assertions.assertEquals(1, partitionExpr.size());
        Assertions.assertTrue(partitionExpr.get(0) instanceof SlotRef);
        SlotRef slotRef = (SlotRef) partitionExpr.get(0);
        Assertions.assertNotNull(slotRef.getSlotDescriptorWithoutCheck());
        SlotDescriptor slotDescriptor = slotRef.getSlotDescriptorWithoutCheck();
        Assertions.assertEquals(1, slotDescriptor.getId().asInt());
    }

    @Test
    public void testHiveMVWithoutPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS select     s_suppkey,     s_nationkey," +
                "sum(s_acctbal) as total_s_acctbal,      count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "supplier_hive_mv");
        Assertions.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assertions.assertTrue(partitionInfo instanceof SinglePartitionInfo);
        Assertions.assertEquals(1, mv.getAllPartitions().size());
        starRocksAssert.dropMaterializedView("supplier_hive_mv");
    }

    @Test
    public void testHiveMVJoinWithoutPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_nation_hive_mv DISTRIBUTED BY " +
                "HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS select     s_suppkey,     n_name,      sum(s_acctbal) " +
                "as total_s_acctbal,      count(s_phone) as s_phone_count from " +
                "hive0.tpch.supplier as supp join hive0.tpch.nation group by s_suppkey, n_name order by s_suppkey;");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "supplier_nation_hive_mv");
        Assertions.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assertions.assertTrue(partitionInfo instanceof SinglePartitionInfo);
        Assertions.assertEquals(1, mv.getAllPartitions().size());
        starRocksAssert.dropMaterializedView("supplier_nation_hive_mv");
    }

    @Test
    public void testHiveMVWithPartition() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lineitem_supplier_hive_mv \n" +
                "partition by l_shipdate\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH MANUAL\n" +
                "AS \n" +
                "select l_shipdate, l_orderkey, l_quantity, l_linestatus, s_name from " +
                "hive0.partitioned_db.lineitem_par join hive0.tpch.supplier where l_suppkey = s_suppkey\n");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "lineitem_supplier_hive_mv");
        Assertions.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assertions.assertTrue(partitionInfo instanceof ExpressionRangePartitionInfo);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        Assertions.assertEquals(1, expressionRangePartitionInfo.getPartitionColumns(table.getIdToColumn()).size());
        Column partColumn = expressionRangePartitionInfo.getPartitionColumns(table.getIdToColumn()).get(0);
        Assertions.assertEquals("l_shipdate", partColumn.getName());
        Assertions.assertTrue(partColumn.getType().isDate());
        starRocksAssert.dropMaterializedView("lineitem_supplier_hive_mv");
    }

    @Test
    public void testHiveMVAsyncRefresh() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH ASYNC  START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "AS select     s_suppkey,     s_nationkey, sum(s_acctbal) as total_s_acctbal,      " +
                "count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "supplier_hive_mv");
        Assertions.assertTrue(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Assertions.assertTrue(partitionInfo instanceof SinglePartitionInfo);
        Assertions.assertEquals(1, mv.getAllPartitions().size());
        MaterializedView.MvRefreshScheme mvRefreshScheme = mv.getRefreshScheme();
        Assertions.assertEquals(mvRefreshScheme.getType(), MaterializedView.RefreshType.ASYNC);
        MaterializedView.AsyncRefreshContext asyncRefreshContext = mvRefreshScheme.getAsyncRefreshContext();
        Assertions.assertEquals(asyncRefreshContext.getTimeUnit(), "HOUR");
        starRocksAssert.dropMaterializedView("supplier_hive_mv");
    }

    @Test
    public void testHiveMVAsyncRefreshWithException() {
        Throwable exception = assertThrows(DdlException.class, () ->
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                    "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH ASYNC AS select     s_suppkey,     s_nationkey," +
                    "sum(s_acctbal) as total_s_acctbal,      count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                    "group by s_suppkey, s_nationkey order by s_suppkey;"));
        assertThat(exception.getMessage(), containsString("Materialized view which type is ASYNC need to specify refresh interval " +
                "for external table"));
    }

    /**
     * Create MV on external catalog should report the correct error message
     */
    @Test
    public void testExternalCatalogException() throws Exception {
        // 1. create mv with full database name
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW default_catalog.test.supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                "from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        starRocksAssert.dropMaterializedView("default_catalog.test.supplier_hive_mv");

        // create mv with database.table
        starRocksAssert.useCatalog("hive0");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.supplier_hive_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                "from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;");
        starRocksAssert.dropMaterializedView("default_catalog.test.supplier_hive_mv");

        // create mv with table name
        AnalysisException ex = Assertions.assertThrows(AnalysisException.class, () ->
                starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW supplier_hive_mv " +
                        "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                        "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                        "from hive0.tpch.supplier as supp " +
                        "group by s_suppkey, s_nationkey order by s_suppkey;")
        );
        Assertions.assertEquals("Getting analyzing error. Detail message: No database selected. " +
                        "You could set the database name through `<database>.<table>` or `use <database>` statement.",
                ex.getMessage());

        // create mv with wrong catalog
        ex = Assertions.assertThrows(AnalysisException.class, () ->
                starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW hive0.tpch.supplier_hive_mv " +
                        "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL AS " +
                        "select s_suppkey, s_nationkey, sum(s_acctbal) as total_s_acctbal " +
                        "from hive0.tpch.supplier as supp " +
                        "group by s_suppkey, s_nationkey order by s_suppkey;")
        );
        Assertions.assertEquals("Getting analyzing error from line 1, column 25 to line 1, column 36. " +
                        "Detail message: Materialized view can only be created in default_catalog. " +
                        "You could either create it with default_catalog.<database>.<mv>, " +
                        "or switch to default_catalog through `set catalog <default_catalog>` statement.",
                ex.getMessage());

        // reset state
        starRocksAssert.useCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        starRocksAssert.useDatabase(testDb.getFullName());
    }

    @Test
    public void testJdbcTable() throws Exception {
        starRocksAssert.withResource("create external resource jdbc0\n" +
                "properties (\n" +
                "    \"type\"=\"jdbc\",\n" +
                "    \"user\"=\"postgres\",\n" +
                "    \"password\"=\"changeme\",\n" +
                "    \"jdbc_uri\"=\"jdbc:postgresql://127.0.0.1:5432/jdbc_test\",\n" +
                "    \"driver_url\"=\"https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar\",\n" +
                "    \"driver_class\"=\"org.postgresql.Driver\"\n" +
                "); ");
        starRocksAssert.withTable("create external table jdbc_tbl (\n" +
                "     `id` bigint NULL,\n" +
                "     `data` varchar(200) NULL\n" +
                " ) ENGINE=jdbc\n" +
                " properties (\n" +
                "     \"resource\"=\"jdbc0\",\n" +
                "     \"table\"=\"dest_tbl\"\n" +
                " );");
        starRocksAssert.withMaterializedView("create materialized view mv_jdbc " +
                "distributed by hash(id) refresh deferred manual " +
                "as select * from jdbc_tbl;");
    }

    @Test
    public void testCreateRealtimeMV() throws Exception {
        String sql = "create materialized view rtmv \n" +
                "refresh incremental " +
                "distributed by hash(l_shipdate) " +
                " as select l_shipdate, l_orderkey, l_quantity, l_linestatus, s_name from " +
                "hive0.partitioned_db.lineitem_par join hive0.tpch.supplier where l_suppkey = s_suppkey\n";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateSyncMvFromSubquery() {
        String sql = "create materialized view sync_mv_1 as" +
                " select k1, sum(k2) from (select k1, k2 from tbl1 group by k1, k2) a group by k1";
        try {
            starRocksAssert.withMaterializedView(sql);
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage().contains("Materialized view query statement only support direct query from table"));
        }
    }

    @Test
    public void testCreateAsyncMv() {
        String sql = "create materialized view async_mv_1 distributed by hash(c_1_9) as" +
                " select c_1_9, c_1_4 from t1";
        try {
            starRocksAssert.withMaterializedView(sql);
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "async_mv_1");
            Assertions.assertTrue(mv.getFullSchema().get(0).isKey());
            Assertions.assertFalse(mv.getFullSchema().get(1).isKey());
        } catch (Exception e) {
            Assertions.fail();
        }

        String sql2 = "create materialized view async_mv_1 distributed by hash(c_1_4) as" +
                " select c_1_4 from t1";
        try {
            starRocksAssert.withMaterializedView(sql2);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("All columns of materialized view cannot be used for keys."));
        }
    }

    @Test
    public void testCollectAllTableAndView() {
        String sql = "select k2,v1 from test.tbl1 where k2 > 0 and v1 not in (select v1 from test.tbl2 where k2 > 0);";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Map<TableName, Table> result = AnalyzerUtils.collectAllTableAndView(statementBase);
            Assertions.assertEquals(result.size(), 2);
        } catch (Exception e) {
            LOG.error("Test CollectAllTableAndView failed", e);
            Assertions.fail();
        }
    }

    @Test
    public void testCreateMVWithDifferentDB() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");
            String sql = "create materialized view test.test_mv_use_different_tbl " +
                    "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
            CreateMaterializedViewStmt stmt =
                    (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, newStarRocksAssert.getCtx());
            Assertions.assertEquals(stmt.getDBName(), "test");
            Assertions.assertEquals(stmt.getMVName(), "test_mv_use_different_tbl");
            currentState.getLocalMetastore().createMaterializedView(stmt);
            waitingRollupJobV2Finish();

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "tbl5");
            Assertions.assertNotNull(table);
            OlapTable olapTable = (OlapTable) table;
            Assertions.assertTrue(olapTable.getIndexIdToMeta().size() >= 2);
            Assertions.assertTrue(olapTable.getIndexIdToMeta().entrySet().stream()
                    .anyMatch(x -> x.getValue().getKeysType().isAggregationFamily()));
            newStarRocksAssert.dropDatabase("test_mv_different_db");
            starRocksAssert.dropMaterializedView("test_mv_use_different_tbl");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateMVWithDifferentDB2() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");

            Assertions.assertThrows(AnalysisException.class, () -> {
                String sql = "create materialized view test_mv_different_db.test_mv_use_different_tbl " +
                        "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
                CreateMaterializedViewStmt stmt =
                        (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                                newStarRocksAssert.getCtx());

            });
            newStarRocksAssert.dropDatabase("test_mv_different_db");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateAsyncMVWithDifferentDB() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");
            String sql = "create materialized view test.test_mv_use_different_tbl " +
                    "distributed by hash(k1) " +
                    "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            newStarRocksAssert.getCtx());
            Assertions.assertEquals(stmt.getTableName().getDb(), "test");
            Assertions.assertEquals(stmt.getTableName().getTbl(), "test_mv_use_different_tbl");

            currentState.getLocalMetastore().createMaterializedView(stmt);
            newStarRocksAssert.dropDatabase("test_mv_different_db");
            Table mv1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "test_mv_use_different_tbl");
            Assertions.assertTrue(mv1 instanceof MaterializedView);
            starRocksAssert.dropMaterializedView("test_mv_use_different_tbl");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateAsyncMVWithDifferentDB2() {
        try {
            ConnectContext newConnectContext = UtFrameUtils.createDefaultCtx();
            StarRocksAssert newStarRocksAssert = new StarRocksAssert(newConnectContext);
            newStarRocksAssert.withDatabase("test_mv_different_db")
                    .useDatabase("test_mv_different_db");
            String sql = "create materialized view test_mv_different_db.test_mv_use_different_tbl " +
                    "distributed by hash(k1) " +
                    "as select k1, sum(v1), min(v2) from test.tbl5 group by k1;";
            CreateMaterializedViewStatement stmt =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                            newStarRocksAssert.getCtx());
            Assertions.assertEquals(stmt.getTableName().getDb(), "test_mv_different_db");
            Assertions.assertEquals(stmt.getTableName().getTbl(), "test_mv_use_different_tbl");

            currentState.getLocalMetastore().createMaterializedView(stmt);

            Database differentDb = currentState.getLocalMetastore().getDb("test_mv_different_db");
            Table mv1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(differentDb.getFullName(), "test_mv_use_different_tbl");
            Assertions.assertTrue(mv1 instanceof MaterializedView);

            newStarRocksAssert.dropDatabase("test_mv_different_db");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateSyncMVWithCaseWhenComplexExpression1() {
        try {
            String t1 = "CREATE TABLE case_when_t1 (\n" +
                    "    k1 INT,\n" +
                    "    k2 char(20))\n" +
                    "DUPLICATE KEY(k1)\n" +
                    "DISTRIBUTED BY HASH(k1)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n";
            starRocksAssert.withTable(t1);
            String mv1 = "create materialized view case_when_mv1 AS SELECT k1, " +
                    "(CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_t1;\n";
            starRocksAssert.withMaterializedView(mv1);
            waitingRollupJobV2Finish();

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "case_when_t1");
            Assertions.assertNotNull(table);
            OlapTable olapTable = (OlapTable) table;
            Assertions.assertTrue(olapTable.getIndexIdToMeta().size() >= 2);
            Assertions.assertTrue(olapTable.getIndexIdToMeta().entrySet().stream()
                    .noneMatch(x -> x.getValue().getKeysType().isAggregationFamily()));
            List<Column> fullSchemas = table.getFullSchema();
            Assertions.assertTrue(fullSchemas.size() == 3);
            Column mvColumn = fullSchemas.get(2);
            Assertions.assertTrue(mvColumn.getName().equals("mv_city"));
            Assertions.assertTrue(mvColumn.getType().isVarchar());
            Assertions.assertTrue(mvColumn.getType().getColumnSize() == 1048576);
            starRocksAssert.dropTable("case_when_t1");
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCreateAsync_Deferred(@Mocked TaskManager taskManager) throws Exception {
        new Expectations() {
            {
                taskManager.executeTask((String) any);
                times = 0;
            }
        };
        starRocksAssert.withMaterializedView(
                "create materialized view deferred_async " +
                        "refresh deferred async distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view deferred_manual " +
                        "refresh deferred manual distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view deferred_scheduled " +
                        "refresh deferred async every(interval 1 day) distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
    }

    @Test
    public void testCreateAsync_Immediate(@Mocked TaskManager taskManager) throws Exception {
        new Expectations() {
            {
                taskManager.executeTask((String) any);
                times = 3;
            }
        };
        starRocksAssert.withMaterializedView(
                "create materialized view async_immediate " +
                        "refresh immediate async distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view manual_immediate " +
                        "refresh immediate manual distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view schedule_immediate " +
                        "refresh immediate async every(interval 1 day) distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
    }

    @Test
    public void testCreateAsync_Immediate_Implicit(@Mocked TaskManager taskManager) throws Exception {
        new Expectations() {
            {
                taskManager.executeTask((String) any);
                times = 3;
            }
        };
        starRocksAssert.withMaterializedView(
                "create materialized view async_immediate_implicit " +
                        "refresh async distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view manual_immediate_implicit " +
                        "refresh manual distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
        starRocksAssert.withMaterializedView(
                "create materialized view schedule_immediate_implicit " +
                        "refresh async every(interval 1 day) distributed by hash(c_1_9) as" +
                        " select c_1_9, c_1_4 from t1");
    }

    private void testMVColumnAlias(String expr) throws Exception {
        String mvName = "mv_alias";
        try {
            String createMvExpr =
                    String.format("create materialized view %s " +
                            "refresh deferred manual distributed by hash(c_1_9) as" +
                            " select c_1_9, %s from t1", mvName, expr);
            starRocksAssert.withMaterializedView(createMvExpr);
            Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), mvName);
            List<String> columnNames = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
            Assertions.assertTrue(columnNames.contains(expr), columnNames.toString());
        } finally {
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testExprAlias() throws Exception {
        testMVColumnAlias("c_1_9 + 1");
        testMVColumnAlias("char_length(c_1_9)");
        testMVColumnAlias("(char_length(c_1_9)) + 1");
        testMVColumnAlias("(char_length(c_1_9)) + '$'");
        testMVColumnAlias("c_1_9 + c_1_10");
    }

    @Test
    public void testMvNullable() throws Exception {
        starRocksAssert.withTable("create table emps (\n" +
                        "    empid int not null,\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null,\n" +
                        "    salary double\n" +
                        ")\n" +
                        "distributed by hash(`empid`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("create table depts (\n" +
                        "    deptno int not null,\n" +
                        "    name varchar(25) not null\n" +
                        ")\n" +
                        "distributed by hash(`deptno`) buckets 10\n" +
                        "properties (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
        {
            starRocksAssert.withMaterializedView("create materialized view mv_nullable" +
                    " distributed by hash(`empid`) as" +
                    " select empid, d.deptno, d.name" +
                    " from emps e left outer join depts d on e.deptno = d.deptno");
            MaterializedView mv = getMv("test", "mv_nullable");
            Assertions.assertFalse(mv.getColumn("empid").isAllowNull());
            Assertions.assertTrue(mv.getColumn("deptno").isAllowNull());
            starRocksAssert.dropMaterializedView("mv_nullable");
        }

        {
            starRocksAssert.withMaterializedView("create materialized view mv_nullable" +
                    " distributed by hash(`empid`) as" +
                    " select empid, d.deptno, d.name" +
                    " from emps e right outer join depts d on e.deptno = d.deptno");
            MaterializedView mv = getMv("test", "mv_nullable");
            Assertions.assertTrue(mv.getColumn("empid").isAllowNull());
            Assertions.assertFalse(mv.getColumn("deptno").isAllowNull());
            Assertions.assertFalse(mv.getColumn("name").isAllowNull());
            starRocksAssert.dropMaterializedView("mv_nullable");
        }

        {
            starRocksAssert.withMaterializedView("create materialized view mv_nullable" +
                    " distributed by hash(`empid`) as" +
                    " select empid, d.deptno, d.name" +
                    " from emps e full outer join depts d on e.deptno = d.deptno");
            MaterializedView mv = getMv("test", "mv_nullable");
            Assertions.assertTrue(mv.getColumn("empid").isAllowNull());
            Assertions.assertTrue(mv.getColumn("deptno").isAllowNull());
            starRocksAssert.dropMaterializedView("mv_nullable");
        }

        starRocksAssert.dropTable("emps");
        starRocksAssert.dropTable("depts");
    }

    @Test
    public void testCreateAsyncDateTruncAndTimeSLice() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 minute) as k11, k2 s2 from tbl1 tb1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 year) as k11, k2 s2 from tbl1 tb1;";
            Assertions.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 month) as k11, k2 s2 from tbl1 tb1;";
            Assertions.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k11)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                    "') EVERY(INTERVAL 3 DAY)\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select time_slice(tb1.k1, interval 5 month, 'ceil') as k11, k2 s2 from tbl1 tb1;";
            Assertions.assertThrows(AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        }
    }

    @Test
    public void testMVWithMaxRewriteStaleness() throws Exception {
        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
        String sql = "create materialized view mv_with_rewrite_staleness \n" +
                "partition by date_trunc('month',k1)\n" +
                "distributed by hash(s2) buckets 10\n" +
                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                "') EVERY(INTERVAL 3 minute)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"," +
                "\"mv_rewrite_staleness_second\" = \"60\"\n" +
                ")\n" +
                "as select tb1.k1, k2 s2 from tbl1 tb1;";
        try {
            Table mv1 = getMaterializedViewChecked(sql);
            // test partition
            MaterializedView materializedView = (MaterializedView) mv1;
            Assertions.assertEquals(materializedView.getMaxMVRewriteStaleness(), 60);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            dropMv("mv_with_rewrite_staleness");
        }
    }

    @Test
    public void testCreateMvWithView() throws Exception {
        starRocksAssert.withView("create view view_1 as select tb1.k1, k2 s2 from tbl1 tb1;");
        starRocksAssert.withView("create view view_2 as select v1.k1, v1.s2 from view_1 v1;");
        starRocksAssert.withView("create view view_3 as select date_trunc('month',k1) d1, v1.s2 from view_1 v1;");

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month', k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select * from view_1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select k1, s2 from view_1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v1.k1, v1.s2 from view_1 v1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select date_trunc('month',k1) d1, v1.s2 from view_1 v1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v3.d1, v3.s2 from view_3 v3;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v2.k1, v2.s2 from view_2 v2;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }
        starRocksAssert.dropView("view_1");
        starRocksAssert.dropView("view_2");
        starRocksAssert.dropView("view_3");
    }

    @Test
    public void testMvOnUnion() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `customer_nullable_1` (\n" +
                "  `c_custkey` int(11) NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26) NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41) NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11) NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16) NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13) NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `customer_nullable_2` (\n" +
                "  `c_custkey` int(11) NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26) NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41) NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11) NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16) NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13) NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("\n" +
                "CREATE TABLE `customer_nullable_3` (\n" +
                "  `c_custkey` int(11)  NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26)  NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41)  NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11)  NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16)  NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13)  NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\",\n" +
                "  `c_total` decimal(19,6) null default \"0.0\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withMaterializedView("\n" +
                "create materialized view customer_mv\n" +
                "distributed by hash(`custkey`)\n" +
                "as\n" +
                "\n" +
                "select\n" +
                "\tc_custkey custkey,\n" +
                "\tc_name name,\n" +
                "\tc_phone phone,\n" +
                "\t0 total,\n" +
                "\t c_mktsegment segment\n" +
                "from customer_nullable_1\n" +
                "\n" +
                "union all\n" +
                "\n" +
                "select\n" +
                "\tc_custkey custkey,\n" +
                "\tnull name,\n" +
                "\tnull phone,\n" +
                "\t0 total,\n" +
                "\t c_mktsegment segment\n" +
                "from customer_nullable_2\n" +
                "\n" +
                "union all\n" +
                "\n" +
                "select\n" +
                "\tc_custkey custkey,\n" +
                "\tnull name,\n" +
                "\tnull phone,\n" +
                "\tc_total total,\n" +
                "\t c_mktsegment segment\n" +
                "from customer_nullable_3;");

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");

        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), "customer_mv");
        Assertions.assertTrue(mv.getColumn("total").getType().isDecimalOfAnyVersion());
        Assertions.assertFalse(mv.getColumn("segment").isAllowNull());
    }

    @Test
    public void testRandomizeStart() throws Exception {
        // NOTE: if the test case execute super slow, the delta would not be so stable
        final long FIXED_DELTA = 5;
        String sql = "create materialized view mv_test_randomize \n" +
                "distributed by hash(k1) buckets 10\n" +
                "refresh async every(interval 1 minute) " +
                "PROPERTIES (\n" +
                "'replication_num' = '1'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        long currentSecond = Utils.getLongFromDateTime(LocalDateTime.now());
        starRocksAssert.withMaterializedView(sql);
        MaterializedView mv = getMv(testDb.getFullName(), "mv_test_randomize");
        long startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
        long delta = startTime - currentSecond;
        Assertions.assertTrue(delta >= 0 && delta <= 60, "delta is " + delta);
        starRocksAssert.dropMaterializedView("mv_test_randomize");

        // manual disable it
        sql = "create materialized view mv_test_randomize \n" +
                "distributed by hash(k1) buckets 10\n" +
                "refresh async every(interval 1 minute) " +
                "PROPERTIES (\n" +
                "'replication_num' = '1', " +
                "'mv_randomize_start' = '-1'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        currentSecond = Utils.getLongFromDateTime(LocalDateTime.now());
        starRocksAssert.withMaterializedView(sql);
        mv = getMv(testDb.getFullName(), "mv_test_randomize");
        startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
        delta = startTime - currentSecond;
        Assertions.assertTrue(delta >= 0 && delta < FIXED_DELTA, "delta is " + delta);
        starRocksAssert.dropMaterializedView("mv_test_randomize");

        // manual specify it
        sql = "create materialized view mv_test_randomize \n" +
                "distributed by hash(k1) buckets 10\n" +
                "refresh async every(interval 1 minute) " +
                "PROPERTIES (\n" +
                "'replication_num' = '1', " +
                "'mv_randomize_start' = '2'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        currentSecond = Utils.getLongFromDateTime(LocalDateTime.now());
        starRocksAssert.withMaterializedView(sql);
        mv = getMv(testDb.getFullName(), "mv_test_randomize");
        startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
        delta = startTime - currentSecond;
        Assertions.assertTrue(delta >= 0 && delta < (2 + FIXED_DELTA), "delta is " + delta);
        starRocksAssert.dropMaterializedView("mv_test_randomize");
    }

    @Test
    public void testRandomizeStartWithStartTime() {
        // NOTE: if the test case execute super slow, the delta would not be so stable
        final long FIXED_DELTA = 5;
        final long FIXED_PERIOD = 60;
        LocalDateTime defineStartTime = LocalDateTime.parse("2023-12-29T17:50:00");
        {
            String sql = "create materialized view mv_test_randomize_with_start_time \n" +
                    "distributed by hash(k1) buckets 10\n" +
                    "refresh async start ('2023-12-29 17:50:00') every(interval 1 minute) " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1'" +
                    ")\n" +
                    "as " +
                    "select tb1.k1, k2, " +
                    "array<int>[1,2,3] as type_array, " +
                    "map<int, int>{1:2} as type_map, " +
                    "parse_json('{\"a\": 1}') as type_json, " +
                    "row('c') as type_struct, " +
                    "array<json>[parse_json('{}')] as type_array_json " +
                    "from tbl1 tb1;";
            long currentSecond = Utils.getLongFromDateTime(defineStartTime);
            starRocksAssert.withMaterializedView(sql, (obj) -> {
                String mvName = (String) obj;
                MaterializedView mv = getMv(testDb.getFullName(), mvName);
                long startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
                long  delta = startTime - currentSecond;
                Assertions.assertTrue(delta >= 0 && delta <= FIXED_DELTA, "delta is " + delta);
            });
        }

        // manual disable it
        {
            String sql = "create materialized view mv_test_randomize_with_start_time \n" +
                    "distributed by hash(k1) buckets 10\n" +
                    "refresh async start ('2023-12-29 17:50:00') every(interval 1 minute) " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1', " +
                    "'mv_randomize_start' = '0'" +
                    ")\n" +
                    "as " +
                    "select tb1.k1, k2, " +
                    "array<int>[1,2,3] as type_array, " +
                    "map<int, int>{1:2} as type_map, " +
                    "parse_json('{\"a\": 1}') as type_json, " +
                    "row('c') as type_struct, " +
                    "array<json>[parse_json('{}')] as type_array_json " +
                    "from tbl1 tb1;";
            final long currentSecond = Utils.getLongFromDateTime(defineStartTime);
            starRocksAssert.withMaterializedView(sql, (obj) -> {
                String mvName = (String) obj;
                MaterializedView mv = getMv(testDb.getFullName(), mvName);
                long startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
                long  delta = startTime - currentSecond;
                Assertions.assertTrue(delta >= 0 && delta <= FIXED_DELTA, "delta is " + delta);
            });
        }

        // manual specify it
        {
            String sql = "create materialized view mv_test_randomize \n" +
                    "distributed by hash(k1) buckets 10\n" +
                    "refresh async start ('2023-12-29 17:50:00') every(interval 1 minute) " +
                    "PROPERTIES (\n" +
                    "'replication_num' = '1', " +
                    "'mv_randomize_start' = '2'" +
                    ")\n" +
                    "as " +
                    "select tb1.k1, k2, " +
                    "array<int>[1,2,3] as type_array, " +
                    "map<int, int>{1:2} as type_map, " +
                    "parse_json('{\"a\": 1}') as type_json, " +
                    "row('c') as type_struct, " +
                    "array<json>[parse_json('{}')] as type_array_json " +
                    "from tbl1 tb1;";
            final long currentSecond = Utils.getLongFromDateTime(defineStartTime);
            starRocksAssert.withMaterializedView(sql, (obj) -> {
                String mvName = (String) obj;
                MaterializedView mv = getMv(testDb.getFullName(), mvName);
                long startTime = mv.getRefreshScheme().getAsyncRefreshContext().getStartTime();
                long  delta = startTime - currentSecond;
                Assertions.assertTrue(delta >= 0 && delta < (2 + FIXED_DELTA), "delta is " + delta);
            });
        }
    }

    @Test
    public void testCreateMvWithTypes() throws Exception {
        String sql = "create materialized view mv_test_types \n" +
                "distributed by hash(k1) buckets 10\n" +
                "PROPERTIES (\n" +
                "'replication_num' = '1'" +
                ")\n" +
                "as " +
                "select tb1.k1, k2, " +
                "array<int>[1,2,3] as type_array, " +
                "map<int, int>{1:2} as type_map, " +
                "parse_json('{\"a\": 1}') as type_json, " +
                "row('c') as type_struct, " +
                "array<json>[parse_json('{}')] as type_array_json " +
                "from tbl1 tb1;";
        starRocksAssert.withMaterializedView(sql);
    }

    @Test
    public void testCreateMaterializedViewOnListPartitionTables1() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "partition by province " +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt, province, avg(age) from list_partition_tbl1 group by dt, province;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testCreateMaterializedViewOnListPartitionTables2() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt, province, avg(age) from list_partition_tbl1 group by dt, province;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testCreateMaterializedViewOnListPartitionTables3() {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) \n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM;";
        starRocksAssert.withTable(
                createSQL,
                () -> {
                    String sql = "create materialized view list_partition_mv1 " +
                            "partition by (province) " +
                            "distributed by hash(dt, province) buckets 10 " +
                            "as select dt, province, avg(age) from list_partition_tbl1 group by dt, province;";
                    try {
                        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
                    } catch (Exception e) {
                        Assertions.fail(e.getMessage());
                    }
                });
    }

    @Test
    public void testCreateMaterializedViewWithTableAlias1() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select t0.k1, t0.k2, t0.sum as sum0 " +
                "from (select k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t0 where t0.k2 > 10";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMaterializedViewWithTableAlias2() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select t0.k1, t0.k2, t0.sum as sum0, t1.sum as sum1, t2.sum as sum2 " +
                "from (select k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t0 " +
                "left join (select  k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t1 on t0.k1=t1.k2 " +
                "left join (select k1, k2, sum(v1) as sum from tbl1 group by k1, k2) t2 on t0.k1=t2.k1;";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testCreateMvWithViewAndSubQuery() throws Exception {
        starRocksAssert.withView("create view view_1 as " +
                "select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10;");
        starRocksAssert.withView("create view view_2 as " +
                "select k1, s2 from (select v1.k1, v1.s2 from view_1 v1) t where t.k1 > 10;");
        starRocksAssert.withView("create view view_3 as " +
                "select d1, s2 from (select date_trunc('month',k1) d1, v1.s2 from view_1 v1)t where d1 is not null;");
        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select k1, s2 from view_1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select view_1.k1, view_2.s2 from view_1 join view_2 on view_1.k1=view_2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select v3.d1, v3.s2 from view_3 v3;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as select view_1.k1, view_2.s2 from view_1 join view_2 on view_1.k1=view_2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        starRocksAssert.dropView("view_1");
        starRocksAssert.dropView("view_2");
        starRocksAssert.dropView("view_3");
    }

    MaterializedView getMaterializedViewChecked(String sql) {
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;

            currentState.getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
            ThreadUtil.sleepAtLeastIgnoreInterrupts(4000L);

            TableName mvName = createMaterializedViewStatement.getTableName();
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), mvName.getTbl());
            Assertions.assertNotNull(table);
            Assertions.assertTrue(table instanceof MaterializedView);
            return (MaterializedView) table;
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        return null;
    }

    List<Column> getMaterializedViewKeysChecked(String sql) {
        String mvName = null;
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;

            currentState.getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
            ThreadUtil.sleepAtLeastIgnoreInterrupts(4000L);

            TableName mvTableName = createMaterializedViewStatement.getTableName();
            mvName = mvTableName.getTbl();

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(testDb.getFullName(), mvName);
            Assertions.assertNotNull(table);
            Assertions.assertTrue(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;

            return mv.getFullSchema().stream().filter(Column::isKey).collect(Collectors.toList());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        } finally {
            if (!Objects.isNull(mvName)) {
                try {
                    starRocksAssert.dropMaterializedView(mvName);
                } catch (Exception e) {
                    Assertions.fail();
                }
            }
        }
        return Lists.newArrayList();
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar1() {
        // sort key by default
        String sql = "create materialized view test_mv1 " +
                "partition by c_1_3 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assertions.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
        Assertions.assertTrue(keyColumns.get(3).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar2() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "partition by c_1_3 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assertions.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
        Assertions.assertTrue(keyColumns.get(3).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar3() {
        // sort key by default
        String sql = "create materialized view test_mv1 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assertions.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
        Assertions.assertTrue(keyColumns.get(3).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar4() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assertions.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
        Assertions.assertTrue(keyColumns.get(3).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar5() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "partition by date_trunc('day', c_1_3) " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assertions.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
        Assertions.assertTrue(keyColumns.get(3).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithSelectStar6() {
        // sort key by default
        String sql = "create materialized view test_mv2 " +
                "partition by date_trunc('month', c_1_3) " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select * from t1 union all select * from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_1"));
        Assertions.assertTrue(keyColumns.get(2).getName().equals("c_1_2"));
        Assertions.assertTrue(keyColumns.get(3).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_Partitioned_1() {
        // sort key by default
        String sql = "create materialized view test_mv_sort_key1 " +
                "partition by c_1_3 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_3, c_1_0, c_1_4, c_1_5 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_0"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_Partitioned_2() {
        // random distribution : double key is in the first
        String sql = "create materialized view test_mv_sort_key1 " +
                "partition by c_1_3 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_3, c_1_0 , c_1_4, c_1_5 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_0"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_Partitioned_3() {
        // random distribution : double key is in the first
        String sql = "create materialized view test_mv_sort_key1 " +
                "partition by c_1_3 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_5, c_1_3, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_0"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_Partitioned_4() {
        // order by with double columns
        String sql = "create materialized view test_mv_sort_key1 " +
                "partition by c_1_3 " +
                "distributed by random " +
                "order by (c_1_0, c_1_3)  " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_5, c_1_3, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_Partitioned_5() {
        // random distribution : double key is in the first, c_1_10 is varchar
        String sql = "create materialized view test_mv_sort_key1 " +
                "partition by c_1_3 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_5, c_1_10, c_1_3, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_10"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_Partitioned_6() {
        // random distribution : double key is in the first, c_1_10 is varchar
        String sql = "create materialized view test_mv_sort_key1 " +
                "partition by c_1_3 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_3, c_1_10, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_10"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_UnPartitioned_1() {
        // sort key by default
        String sql = "create materialized view test_mv_sort_key1 " +
                "distributed by hash(c_1_3, c_1_0) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_3, c_1_0, c_1_4, c_1_5 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_0"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_UnPartitioned_2() {
        // random distribution : double key is in the first
        String sql = "create materialized view test_mv_sort_key1 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_3, c_1_0 , c_1_4, c_1_5 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_0"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_UnPartitioned_3() {
        // random distribution : double key is in the first
        String sql = "create materialized view test_mv_sort_key1 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_5, c_1_3, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_0"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_UnPartitioned_4() {
        // order by with double columns
        String sql = "create materialized view test_mv_sort_key1 " +
                "distributed by random " +
                "order by (c_1_0, c_1_3)  " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_5, c_1_3, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_0"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_3"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_UnPartitioned_5() {
        // random distribution : double key is in the first, c_1_10 is varchar
        String sql = "create materialized view test_mv_sort_key1 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_5, c_1_10, c_1_3, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_10"));
    }

    @Test
    public void testCreateMaterializedViewWithoutSortKeys_UnPartitioned_6() {
        // random distribution : double key is in the first, c_1_10 is varchar
        String sql = "create materialized view test_mv_sort_key1 " +
                "distributed by random " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select c_1_4, c_1_3, c_1_10, c_1_0 from t1";
        List<Column> keyColumns = getMaterializedViewKeysChecked(sql);
        Assertions.assertTrue(keyColumns.get(0).getName().equals("c_1_3"));
        Assertions.assertTrue(keyColumns.get(1).getName().equals("c_1_10"));
    }

    @Test
    public void createDeltaLakeMV() throws Exception {
        new MockUp<DeltaLakeTable>() {
            @Mock
            public String getTableIdentifier() {
                String uuid = UUID.randomUUID().toString();
                return Joiner.on(":").join("tbl", uuid);
            }
        };
        starRocksAssert.withMaterializedView("create materialized view mv_deltalake " +
                " refresh manual" +
                " as select * from deltalake_catalog.deltalake_db.tbl");
    }

    @Test
    public void createPaimonMV() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view mv_paimon " +
                " refresh manual" +
                " as select * from paimon0.pmn_db1.unpartitioned_table");
    }

    @Test
    public void testCreateMvWithUnsupportedStr2date() {
        {
            String sql = "create materialized view mv1 " +
                    "partition by ss " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d, '%m-%d-%Y') ss, a, b, c from jdbc0.partitioned_db0.tbl1;";
            Assertions.assertThrows(AnalysisException.class, () -> starRocksAssert.useDatabase("test").withMaterializedView(sql), "Materialized view partition function date_trunc check failed");
        }

        {
            String sql = "create materialized view mv1 " +
                    "partition by date_trunc('month', ss) " +
                    "distributed by hash(a) buckets 10 " +
                    "REFRESH DEFERRED MANUAL " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select str2date(d, '%m-%d-%Y') ss, a, b, c from jdbc0.partitioned_db0.tbl1;";
            Assertions.assertThrows(AnalysisException.class, () -> starRocksAssert.useDatabase("test").withMaterializedView(sql), "Materialized view partition function date_trunc check failed");
        }
    }

    @Test
    public void testCreateMvWithCTE() throws Exception {
        starRocksAssert.withView("create view view_1 as select tb1.k1, k2 s2 from tbl1 tb1;");
        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select k1, s2 from cte1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select a.k1, a.s2 from cte1 as a;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select cte1.k1, cte2.s2 from cte1 join cte2 on cte1.k1=cte2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by d1\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte3 as (select d1, s2 from (select date_trunc('month',k1) d1" +
                    ", v1.s2 from view_1 v1)t where d1 is not null) " +
                    " select v3.d1, v3.s2 from cte3 v3;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select cte1.k1, cte2.s2 from cte1 join cte2 on cte1.k1=cte2.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select a.k1, b.s2 from cte1 a join cte2 b on a.k1=b.k1;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        }

        {
            String sql = "create materialized view mv1\n" +
                    "partition by date_trunc('month',b.k1)\n" +
                    "distributed by hash(s2) buckets 10\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ")\n" +
                    "as with cte1 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10), " +
                    " cte2 as (select k1, s2 from (select tb1.k1, k2 s2 from tbl1 tb1) t where t.k1 > 10) " +
                    " select a.k1, b.s2 from cte1 a join cte2 b on a.k1=b.k1;";
            Assertions.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext), "Materialized view partition exp: `b`.`k1` must related to column.");
        }
        starRocksAssert.dropView("view_1");
    }

    @Test
    public void testUseSubQueryWithStar1() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select * from (select * from tbl1) tbl";

        starRocksAssert.withMaterializedView(sql, () -> {});
    }

    @Test
    public void testUseSubQueryWithStar2() {
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by random " +
                "refresh deferred manual " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select * from (select * from tbl1 where k1 > '19930101') tbl";
        starRocksAssert.withMaterializedView(sql, () -> {});
    }

    @Test
    public void testUseSubQueryWithStar3() throws Exception {
        starRocksAssert.withView("create view view_1 as select tb1.k1, k2 s2 from tbl1 tb1;",
                () -> {
                    String sql = "create materialized view mv1\n" +
                            "partition by date_trunc('month',k1)\n" +
                            "distributed by hash(k2) buckets 10\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\"\n" +
                            ")\n" +
                            "as with cte1 as (select * from (select * from tbl1 tb1) t where t.k1 > 10) " +
                            " select * from cte1;";
                    starRocksAssert.withMaterializedView(sql, () -> {});
                });
    }

    @Test
    public void testPartitionByWithOrderBy1() {
        starRocksAssert.withTable(new MTable("tt1", "k1",
                        List.of(
                                "k1 datetime",
                                "k2 string",
                                "v1 int"
                        ),

                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                "PARTITION p2 values [('2022-02-01'),('2022-03-01'))",
                                "PARTITION p3 values [('2022-03-01'),('2022-04-01'))",
                                "PARTITION p4 values [('2022-04-01'),('2022-05-01'))"
                        )
                ),
                () -> {
                    String sql = "create materialized view mv1 " +
                            "partition by date_trunc('day', k1) " +
                            "distributed by random " +
                            "order by (k3) \n" +
                            "refresh deferred manual " +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\"\n" +
                            ") " +
                            "as select k1, v1, concat(k2, 'xxx') as k3 from (select * from tt1 where k1 > '19930101') tbl";
                    starRocksAssert.withMaterializedView(sql, () -> {});
                });
    }

    @Test
    public void testCreateMVWithIntervalRefreshTime() {
        starRocksAssert.withTable(new MTable("tt1", "k1",
                        List.of(
                                "k1 datetime",
                                "k2 string",
                                "v1 int"
                        ),

                        "k1",
                        List.of(
                                "PARTITION p0 values [('2021-12-01'),('2022-01-01'))",
                                "PARTITION p1 values [('2022-01-01'),('2022-02-01'))",
                                "PARTITION p2 values [('2022-02-01'),('2022-03-01'))",
                                "PARTITION p3 values [('2022-03-01'),('2022-04-01'))",
                                "PARTITION p4 values [('2022-04-01'),('2022-05-01'))"
                        )
                ),
                () -> {
                    {
                        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
                        String sql = "create materialized view mv1 " +
                                "partition by date_trunc('day', k1) " +
                                "distributed by random " +
                                "order by (k3) \n" +
                                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                                "') EVERY(INTERVAL 30 SECOND)\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\"\n" +
                                ") " +
                                "as select k1, v1, concat(k2, 'xxx') as k3 from (select * from tt1 where k1 > '19930101') tbl";
                        Assertions.assertThrows(AssertionError.class, () -> starRocksAssert.withMaterializedView(sql, () -> {}), "Refresh schedule interval 30 is too small which may cost a lot of memory/cpu " +
                                        "resources to refresh the asynchronous materialized view, " +
                                        "please config an interval larger than " +
                                        "Config.min_allowed_materialized_view_schedule_time(60s).");
                    }
                    {
                        LocalDateTime startTime = LocalDateTime.now().plusSeconds(3);
                        String sql = "create materialized view mv2 " +
                                "partition by date_trunc('day', k1) " +
                                "distributed by random " +
                                "order by (k3) \n" +
                                "refresh async START('" + startTime.format(DateUtils.DATE_TIME_FORMATTER) +
                                "') EVERY(INTERVAL 1 MINUTE)\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\"\n" +
                                ") " +
                                "as select k1, v1, concat(k2, 'xxx') as k3 from (select * from tt1 where k1 > '19930101') tbl";
                        starRocksAssert.withMaterializedView(sql, () -> {});
                    }
                    {
                        String sql = "create materialized view mv3 " +
                                "partition by date_trunc('day', k1) " +
                                "distributed by random " +
                                "refresh async EVERY(INTERVAL 1 SECOND)\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\"\n" +
                                ") " +
                                "as select k1, v1, concat(k2, 'xxx') as k3 from (select * from tt1 where k1 > '19930101') tbl";
                        Exception e = Assertions.assertThrows(DdlException.class,
                                () -> starRocksAssert.withMaterializedView(sql));
                        Assertions.assertEquals("Refresh schedule interval 1 is too small " +
                                "which may cost a lot of memory/cpu resources to refresh the asynchronous " +
                                "materialized view, please config an interval larger than " +
                                "Config.materialized_view_min_refresh_interval(60s).", e.getMessage());

                        // change the limitation
                        int before = Config.materialized_view_min_refresh_interval;
                        Config.materialized_view_min_refresh_interval = 1;
                        starRocksAssert.withMaterializedView(sql);
                        Config.materialized_view_min_refresh_interval = before;
                    }
                });
    }

    /**
     * https://github.com/StarRocks/starrocks/issues/40862
     */
    @Test
    public void testSR40862() throws Exception {
        starRocksAssert.withTable("CREATE TABLE sr_ods_test_table (\n" +
                "id bigint(20) NOT NULL COMMENT '主键id' ,\n" +
                "name string COMMENT '名称'\n" +
                ")\n" +
                "PRIMARY KEY (ID)\n" +
                "DISTRIBUTED BY HASH(ID)");
        starRocksAssert.withRefreshedMaterializedView("CREATE MATERIALIZED VIEW sr_dw_test_table\n" +
                "DISTRIBUTED BY HASH(id)\n" +
                "REFRESH ASYNC\n" +
                "AS\n" +
                "SELECT id,name,str_to_map(CONCAT_WS(':',id,name),';',':') as mapvalue FROM sr_ods_test_table");
    }

    @Test
    public void testEnableQueryRewrite() throws Exception {
        // default
        starRocksAssert.withMaterializedView("create materialized view mv_invalid " +
                "refresh async " +
                "as select * from t1 limit 10");
        Assertions.assertEquals(
                "CREATE MATERIALIZED VIEW `mv_invalid` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, `c_1_4`, " +
                        "`c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1` LIMIT 10;"
                ,
                starRocksAssert.showCreateTable("show create table mv_invalid"));
        starRocksAssert.dropMaterializedView("mv_invalid");

        // disable
        starRocksAssert.withMaterializedView("create materialized view mv_invalid " +
                "refresh async " +
                "properties('enable_query_rewrite' = 'false') " +
                "as select * from t1 limit 10");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_invalid` " +
                        "(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, `c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, " +
                        "`c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"enable_query_rewrite\" = \"false\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1` LIMIT 10;",
                starRocksAssert.showCreateTable("show create table mv_invalid"));
        starRocksAssert.dropMaterializedView("mv_invalid");

        // enable
        starRocksAssert.withMaterializedView("create materialized view mv_enable " +
                "refresh async " +
                "properties('enable_query_rewrite' = 'true') " +
                "as select * from t1");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_enable` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, " +
                        "`c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"enable_query_rewrite\" = \"true\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1`;",
                starRocksAssert.showCreateTable("show create table mv_enable"));
        starRocksAssert.refreshMV("refresh materialized view mv_enable with sync mode");
        MaterializedView mv = starRocksAssert.getMv("test", "mv_enable");
        MVPlanValidationResult valid = MvRewritePreprocessor.isMVValidToRewriteQuery(connectContext, mv,
                null, true, false, connectContext.getSessionVariable().getOptimizerExecuteTimeout());
        Assertions.assertTrue(valid.getStatus().isValid());

        starRocksAssert.ddl("alter materialized view mv_enable set('enable_query_rewrite'='false') ");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_enable` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, " +
                        "`c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"enable_query_rewrite\" = \"FALSE\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1`;",
                starRocksAssert.showCreateTable("show create table mv_enable"));
        valid = MvRewritePreprocessor.isMVValidToRewriteQuery(connectContext, mv, null,
                true, false, connectContext.getSessionVariable().getOptimizerExecuteTimeout());
        Assertions.assertFalse(valid.getStatus().isValid());
        Assertions.assertEquals("enable_query_rewrite=FALSE", valid.getReason());
        starRocksAssert.dropMaterializedView("mv_enable");
    }

    @Test
    public void testEnableTransparentMVRewrite() throws Exception {
        // disable
        starRocksAssert.withMaterializedView("create materialized view mv_invalid " +
                "refresh async " +
                "properties('transparent_mv_rewrite_mode' = 'false') " +
                "as select * from t1 limit 10");
        String sql = starRocksAssert.showCreateTable("show create table mv_invalid");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_invalid` " +
                "(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, `c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, " +
                "`c_1_10`, `c_1_11`, `c_1_12`)\n" +
                "DISTRIBUTED BY RANDOM\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"transparent_mv_rewrite_mode\" = \"false\",\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                "`t1`.`c_1_12`\n" +
                "FROM `test`.`t1` LIMIT 10;", sql);
        starRocksAssert.dropMaterializedView("mv_invalid");

        // enable
        starRocksAssert.withMaterializedView("create materialized view mv_enable " +
                "refresh async " +
                "properties('transparent_mv_rewrite_mode' = 'true') " +
                "as select * from t1");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_enable` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, " +
                        "`c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"transparent_mv_rewrite_mode\" = \"true\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1`;",
                starRocksAssert.showCreateTable("show create table mv_enable"));
        starRocksAssert.refreshMV("refresh materialized view mv_enable with sync mode");
        MaterializedView mv = starRocksAssert.getMv("test", "mv_enable");
        Assertions.assertTrue(mv.isEnableTransparentRewrite());
        Assertions.assertTrue(mv.getTransparentRewriteMode().equals(TableProperty.MVTransparentRewriteMode.TRUE));

        starRocksAssert.ddl("alter materialized view mv_enable set('transparent_mv_rewrite_mode'='false') ");
        sql = starRocksAssert.showCreateTable("show create table mv_enable");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_enable` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, " +
                        "`c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"transparent_mv_rewrite_mode\" = \"FALSE\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1`;", sql);
        Assertions.assertTrue(!mv.isEnableTransparentRewrite());
        Assertions.assertTrue(mv.getTransparentRewriteMode().equals(TableProperty.MVTransparentRewriteMode.FALSE));

        starRocksAssert.ddl("alter materialized view mv_enable set('transparent_mv_rewrite_mode'='transparent_or_error') ");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_enable` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, " +
                        "`c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"transparent_mv_rewrite_mode\" = \"TRANSPARENT_OR_ERROR\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1`;",
                starRocksAssert.showCreateTable("show create table mv_enable"));
        Assertions.assertTrue(mv.isEnableTransparentRewrite());
        Assertions.assertTrue(mv.getTransparentRewriteMode().equals(TableProperty.MVTransparentRewriteMode.TRANSPARENT_OR_ERROR));
        starRocksAssert.ddl("alter materialized view mv_enable set('transparent_mv_rewrite_mode'='TRANSPARENT_OR_DEFAULT') ");
        Assertions.assertEquals("CREATE MATERIALIZED VIEW `mv_enable` (`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`, " +
                        "`c_1_4`, `c_1_5`, `c_1_6`, `c_1_7`, `c_1_8`, `c_1_9`, `c_1_10`, `c_1_11`, `c_1_12`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "REFRESH ASYNC\n" +
                        "PROPERTIES (\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"transparent_mv_rewrite_mode\" = \"TRANSPARENT_OR_DEFAULT\",\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `t1`.`c_1_0`, `t1`.`c_1_1`, `t1`.`c_1_2`, `t1`.`c_1_3`, `t1`.`c_1_4`, `t1`.`c_1_5`, " +
                        "`t1`.`c_1_6`, `t1`.`c_1_7`, `t1`.`c_1_8`, `t1`.`c_1_9`, `t1`.`c_1_10`, `t1`.`c_1_11`, " +
                        "`t1`.`c_1_12`\n" +
                        "FROM `test`.`t1`;",
                starRocksAssert.showCreateTable("show create table mv_enable"));
        Assertions.assertTrue(mv.isEnableTransparentRewrite());
        Assertions.assertTrue(mv.getTransparentRewriteMode().equals(TableProperty.MVTransparentRewriteMode.TRANSPARENT_OR_DEFAULT));
        starRocksAssert.dropMaterializedView("mv_enable");
    }

    @Test
    public void testCreateMVWithLocationAndPersist() throws Exception {
        // add label to backend
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        System.out.println(systemInfoService.getBackends());
        List<Long> backendIds = systemInfoService.getBackendIds();
        Backend backend = systemInfoService.getBackend(backendIds.get(0));
        String modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'rack:rack1')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);

        // add a backend with no location
        UtFrameUtils.addMockBackend(12011);

        // init empty image
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(initialImage.getImageWriter());

        // Create a mv with specific location, we should expect the tablets of that mv will only distribute on
        // that single labeled backend.
        MaterializedView materializedView = getMaterializedViewChecked("create materialized view mv_with_location " +
                "DISTRIBUTED BY HASH(`c_1_0`)\n" +
                "buckets 15\n" +
                "REFRESH MANUAL\n" +
                "properties('labels.location' = 'rack:*') " +
                "as select c_1_0 from t1 limit 10");

        String result = starRocksAssert.showCreateTable("show create table mv_with_location");
        System.out.println(result);
        Assertions.assertTrue(result.contains("rack:*"));
        for (Tablet tablet : materializedView.getPartitions().iterator().next()
                .getDefaultPhysicalPartition().getBaseIndex().getTablets()) {
            Assertions.assertEquals(backend.getId(), (long) tablet.getBackendIds().iterator().next());
        }

        // make final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(finalImage.getImageWriter());

        // test replay
        LocalMetastore localMetastoreFollower = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        localMetastoreFollower.load(new SRMetaBlockReaderV2(initialImage.getJsonReader()));
        CreateTableInfo info = (CreateTableInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_TABLE_V2);
        localMetastoreFollower.replayCreateTable(info);
        MaterializedView mv = (MaterializedView) localMetastoreFollower.getDb("test")
                .getTable("mv_with_location");
        System.out.println(mv.getLocation());
        Assertions.assertEquals(1, mv.getLocation().size());
        Assertions.assertTrue(mv.getLocation().containsKey("rack"));

        // test restart
        LocalMetastore localMetastoreLeader = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        localMetastoreLeader.load(new SRMetaBlockReaderV2(finalImage.getJsonReader()));
        mv = (MaterializedView) localMetastoreLeader.getDb("test")
                .getTable("mv_with_location");
        System.out.println(mv.getLocation());
        Assertions.assertEquals(1, mv.getLocation().size());
        Assertions.assertTrue(mv.getLocation().containsKey("rack"));


        // clean: remove backend 12011
        modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = '')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);
        backend = systemInfoService.getBackend(12011);
        systemInfoService.dropBackend(backend);
    }

    @Test
    public void testCreateListPartitionedMVOfOlap() throws Exception {
        String sql = "CREATE TABLE `s1` (\n" +
                "   `id` varchar(36),\n" +
                "   `location_id` varchar(36),\n" +
                "   `location_id_hash` int,\n" +
                "   `source_id` varchar(36),\n" +
                "   `person_id` varchar(36)\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`,`location_id`,`location_id_hash`)\n" +
                "PARTITION BY (`location_id_hash`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "   \"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(sql);
        String mvSql = "create materialized view test_mv1\n" +
                "PARTITION BY `location_id_hash`\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ") \n" +
                "as select `id`, `location_id`, `location_id_hash` from `s1`;";
        starRocksAssert.withMaterializedView(mvSql, () -> {
            MaterializedView mv = starRocksAssert.getMv("test", "test_mv1");
            Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
        });
        starRocksAssert.dropTable("s1");
    }

    @Test
    public void testCreateListPartitionedMVOfExternal() throws Exception {
        String sql = "create materialized view mv1 " +
                "partition by (d)" +
                "distributed by hash(a) buckets 10 " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select a, b, c, d from jdbc0.partitioned_db0.tbl1;";
        CreateMaterializedViewStatement stmt =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Expr partitionByExpr = getMVPartitionByExprChecked(stmt);
        Assertions.assertTrue(partitionByExpr instanceof SlotRef);
        List<SlotRef> slotRefs = Lists.newArrayList();
        partitionByExpr.collect(SlotRef.class, slotRefs);
        Assertions.assertEquals(partitionByExpr, slotRefs.get(0));
        Assertions.assertEquals("d", slotRefs.get(0).getColumnName());
        starRocksAssert.withMaterializedView(sql, () -> {
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            Assertions.assertTrue(mv.getPartitionInfo().isListPartition());
        });
    }

    @Test
    public void testCreateMVWithMultiPartitionColumns1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        // create mv with multi partition columns
        {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "partition by (province, dt) \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "properties ('partition_refresh_number' = '-1')" +
                    "as select dt, province, sum(age) from t3 group by dt, province;");
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            List<Column> mvPartitionCols = mv.getPartitionColumns();
            Assertions.assertEquals(2, mvPartitionCols.size());
            Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
            Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
            starRocksAssert.dropMaterializedView("mv1");
        }

        // create mv with multi partition columns and partition expressions
        {
            try {
                starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                        "partition by (province, str2date(dt, '%Y%m%d')) \n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "properties ('partition_refresh_number' = '-1')" +
                        "as select dt, province, sum(age) from t3 group by dt, province;");
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertTrue(e.getMessage().contains("List materialized view's partition expression can only refer " +
                        "ref-base-table's partition expression without transforms but contains"));
            }
        }
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithMultiPartitionColumns2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        // create mv with multi partition columns
        {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "partition by (province, dt, age) \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "properties ('partition_refresh_number' = '-1')" +
                    "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            List<Column> mvPartitionCols = mv.getPartitionColumns();
            Assertions.assertEquals(3, mvPartitionCols.size());
            Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
            Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
            Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
            starRocksAssert.dropMaterializedView("mv1");
        }

        // create mv with multi partition columns
        {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "partition by (dt, province, age) \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "properties ('partition_refresh_number' = '-1')" +
                    "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            List<Column> mvPartitionCols = mv.getPartitionColumns();
            Assertions.assertEquals(3, mvPartitionCols.size());
            Assertions.assertEquals("dt", mvPartitionCols.get(0).getName());
            Assertions.assertEquals("province", mvPartitionCols.get(1).getName());
            Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
            starRocksAssert.dropMaterializedView("mv1");
        }

        // create mv with multi partition columns
        {
            try {
                starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                        "partition by (province, dt) \n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "properties ('partition_refresh_number' = '-1')" +
                        "as select dt, province, max(age) from t3 group by dt, province;");
                starRocksAssert.dropMaterializedView("mv1");
            } catch (Exception e) {
                Assertions.fail();
            }
        }

        // create mv with multi partition columns and partition expressions
        {
            try {
                starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                        "partition by (province, str2date(dt, '%Y%m%d')) \n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "properties ('partition_refresh_number' = '-1')" +
                        "as select dt, province, sum(age) from t3 group by dt, province;");
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertTrue(e.getMessage().contains("List materialized view's partition expression can only refer " +
                        "ref-base-table's partition expression without transforms but contains"));
            }
        }
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithMultiPartitionColumns3() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withTable("CREATE TABLE t4 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "partition by (province, dt, age) \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "properties ('partition_refresh_number' = '-1')" +
                    "as " +
                    "select dt, province, age, sum(id) from t3 group by dt, province, age " +
                    "UNION ALL " +
                    "select dt, province, age, sum(id) from t4 group by dt, province, age " +
                    ";");
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            List<Column> mvPartitionCols = mv.getPartitionColumns();
            Assertions.assertEquals(3, mvPartitionCols.size());
            Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
            Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
            Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
            starRocksAssert.dropMaterializedView("mv1");
        }

        {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "partition by (col1, col2, col3) \n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "properties ('partition_refresh_number' = '-1')" +
                    "as " +
                    "select t3.dt col1, t3.province col2, t3.age col3, t3.id col4, " +
                    "   t4.dt col5, t4.province col6, t4.age col7, t4.id col8 " +
                    "   from t3 join t4 \n" +
                    "   on t3.dt=t4.dt and t3.age=t4.age and t3.province=t4.province;" +
                    ";");
            MaterializedView mv = starRocksAssert.getMv("test", "mv1");
            List<Column> mvPartitionCols = mv.getPartitionColumns();
            Assertions.assertEquals(3, mvPartitionCols.size());
            Assertions.assertEquals("col1", mvPartitionCols.get(0).getName());
            Assertions.assertEquals("col2", mvPartitionCols.get(1).getName());
            Assertions.assertEquals("col3", mvPartitionCols.get(2).getName());
            starRocksAssert.dropMaterializedView("mv1");
        }

        {
            try {
                starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                        "partition by (col1, col2, col3) \n" +
                        "REFRESH DEFERRED MANUAL \n" +
                        "properties ('partition_refresh_number' = '-1')" +
                        "as " +
                        "select t3.dt col1, t3.province col2, t3.age col3, t3.id col4, " +
                        "   t4.dt col5, t4.province col6, t4.age col7, t4.id col8 " +
                        "   from t3 join t4 \n" +
                        "   on t3.dt=t4.dt;" +
                        ";");
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertTrue(e.getMessage().contains("The current partition expr maps size 1 should be equal " +
                        "to the size of the first partition expr maps: 2."));
            }
        }
        starRocksAssert.dropTable("t3");
        starRocksAssert.dropTable("t4");
    }

    @Test
    public void testHiveMVInExternalCatalog() throws DdlException {
        try {
            starRocksAssert.useCatalog("hive0");
            String sql = "CREATE MATERIALIZED VIEW tpch.supplier_mv " +
                    "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL " +
                    "AS select s_suppkey, s_nationkey, " +
                    "sum(s_acctbal) as total_s_acctbal, count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                    "group by s_suppkey, s_nationkey order by s_suppkey;";
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Can not find database"));
        } finally {
            starRocksAssert.useCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        }
    }

    @Test
    public void testHiveMVInDefaultCatalog() throws Exception {
        String sql = "CREATE MATERIALIZED VIEW supplier_mv " +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 10 REFRESH MANUAL " +
                "AS select s_suppkey, s_nationkey, " +
                "sum(s_acctbal) as total_s_acctbal, count(s_phone) as s_phone_count from hive0.tpch.supplier as supp " +
                "group by s_suppkey, s_nationkey order by s_suppkey;";
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test
    public void testCreateMVWithAutoRefreshPartitionsLimit() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (province, dt, age) \n" +
                "REFRESH ASYNC\n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                // check auto_refresh_partitions_limit parameter
                "'auto_refresh_partitions_limit' = '1'," +
                "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                ") \n" +
                "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        Assertions.assertEquals(3, mvPartitionCols.size());
        Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
        Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
        Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithAdaptiveRefresh1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (province, dt, age) \n" +
                "REFRESH DEFERRED MANUAL \n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_strategy' = 'adaptive'" +
                ") \n" +
                "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        Assertions.assertEquals(3, mvPartitionCols.size());
        Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
        Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
        Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
        String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_refresh_strategy' = 'strict')";
        starRocksAssert.alterMvProperties(alterTableSql);

        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithAdaptiveRefresh2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (province, dt, age) \n" +
                "REFRESH DEFERRED MANUAL \n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_strategy' = 'strict'" +
                ") \n" +
                "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        Assertions.assertEquals(3, mvPartitionCols.size());
        Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
        Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
        Assertions.assertEquals("age", mvPartitionCols.get(2).getName());

        String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_refresh_strategy' = 'adaptive')";
        starRocksAssert.alterMvProperties(alterTableSql);
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithRetentionCondition1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province, dt, age) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\", \"10\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\", \"20\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\", \"30\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\", \"40\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (province, dt, age) \n" +
                "REFRESH DEFERRED MANUAL \n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_number' = '-1'," +
                "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                ") \n" +
                "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        Assertions.assertEquals(3, mvPartitionCols.size());
        Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
        Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
        Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithRetentionCondition2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY province, dt, age\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (province, dt, age) \n" +
                "REFRESH DEFERRED MANUAL \n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_number' = '-1'," +
                "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                ") \n" +
                "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        Assertions.assertEquals(3, mvPartitionCols.size());
        Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
        Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
        Assertions.assertEquals("age", mvPartitionCols.get(2).getName());
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithRetentionCondition3() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY RANDOM\n");
        try {
            starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                    "REFRESH DEFERRED MANUAL \n" +
                    "properties (\n" +
                    "'replication_num' = '1',\n" +
                    "'partition_refresh_number' = '-1'," +
                    "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                    ") \n" +
                    "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("partition_retention_condition is " +
                    "only supported by partitioned materialized-view."));
        }
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithRetentionCondition4() throws Exception {
        starRocksAssert.withTable("CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10) not null,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY province, dt, age\n" +
                "DISTRIBUTED BY RANDOM\n");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (province, dt, age) \n" +
                "REFRESH DEFERRED MANUAL \n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_number' = '-1'," +
                "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                ") \n" +
                "as select dt, province, age, sum(id) from t3 group by dt, province, age;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        List<Column> mvPartitionCols = mv.getPartitionColumns();
        Assertions.assertEquals(3, mvPartitionCols.size());
        Assertions.assertEquals("province", mvPartitionCols.get(0).getName());
        Assertions.assertEquals("dt", mvPartitionCols.get(1).getName());
        Assertions.assertEquals("age", mvPartitionCols.get(2).getName());

        String retentionCondition = mv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertEquals("dt > current_date() - interval 1 month", retentionCondition);

        try {
            String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_retention_condition' = " +
                    "'last_day(dt) > current_date() - interval 2 month')";
            starRocksAssert.alterMvProperties(alterTableSql);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Retention condition must only contain FE constant functions " +
                    "for materialized view but contains: last_day"));
        }

        String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_retention_condition' = " +
                "'date_format(dt, \\'%m月%Y年\\') > current_date() - interval 2 month')";
        starRocksAssert.alterMvProperties(alterTableSql);

        retentionCondition = mv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertEquals("date_format(dt, '%m月%Y年') > current_date() - interval 2 month", retentionCondition);
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("t3");
    }

    @Test
    public void testCreateMVWithRetentionCondition5() throws Exception {
        starRocksAssert.withTable("CREATE TABLE r1 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(\n" +
                "    PARTITION p0 values [('2024-01-29'),('2024-01-30')),\n" +
                "    PARTITION p1 values [('2024-01-30'),('2024-01-31')),\n" +
                "    PARTITION p2 values [('2024-01-31'),('2024-02-01')),\n" +
                "    PARTITION p3 values [('2024-02-01'),('2024-02-02')) \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "'replication_num' = '1',\n" +
                "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                ")");
        starRocksAssert.withMaterializedView("create materialized view mv1\n" +
                "partition by (dt) \n" +
                "REFRESH DEFERRED MANUAL \n" +
                "properties (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_number' = '-1'," +
                "'partition_retention_condition' = 'dt > current_date() - interval 1 month'\n" +
                ") \n" +
                "as select * from r1;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        String retentionCondition = mv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertEquals("dt > current_date() - interval 1 month", retentionCondition);

        try {

            String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_retention_condition' = " +
                    "'date_format(dt, \\'%m月%Y年\\') > current_date() - interval 2 month')";
            starRocksAssert.alterMvProperties(alterTableSql);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Retention condition must only contain monotonic functions for " +
                    "range partition tables but contains: date_format"));
        }

        String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_retention_condition' = " +
                "'last_day(dt) > current_date() - interval 2 month')";
        starRocksAssert.alterMvProperties(alterTableSql);
        retentionCondition = mv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertEquals("last_day(dt) > current_date() - interval 2 month", retentionCondition);
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("r1");
    }

    @Test
    public void testCreateMVWithMultiCommonPartitionExpressions1() throws Exception {
        starRocksAssert.withTable("create table tt1(k1 datetime, k2 datetime, v int) " +
                "partition by date_trunc('day', k1), date_trunc('month', k2);\n");
        starRocksAssert.withMaterializedView("\n" +
                "CREATE MATERIALIZED VIEW mv1\n" +
                "PARTITION BY (date_trunc('day', k1), date_trunc('month', k2))\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES(\n" +
                "    \"auto_refresh_partitions_limit\"=\"30\"\n" +
                ")\n" +
                "as select * from tt1;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        Assertions.assertTrue(mv != null);
        String alterTableSql = "ALTER MATERIALIZED VIEW mv1 SET ('partition_retention_condition' = " +
                "'date_trunc(\\'day\\', k1) > current_date() - interval 2 month')";
        starRocksAssert.alterMvProperties(alterTableSql);
        String retentionCondition = mv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertEquals("date_trunc('day', k1) > current_date() - interval 2 month", retentionCondition);
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("tt1");
    }

    @Test
    public void testCreateMVWithMultiCommonPartitionExpressions2() throws Exception {
        starRocksAssert.withTable("create table tt1(k1 datetime, k2 datetime, v int) " +
                "partition by date_trunc('day', k1), date_trunc('month', k2);\n");
        starRocksAssert.withMaterializedView("\n" +
                "CREATE MATERIALIZED VIEW mv1\n" +
                "PARTITION BY (date_trunc('day', k1), date_trunc('month', k2))\n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES(\n" +
                "    \"auto_refresh_partitions_limit\"=\"30\",\n" +
                "   'partition_retention_condition' = 'date_trunc(\\'day\\', k1) > current_date() - interval 2 month'" +
                ")\n" +
                "as select * from tt1;");
        MaterializedView mv = starRocksAssert.getMv("test", "mv1");
        Assertions.assertTrue(mv != null);
        String retentionCondition = mv.getTableProperty().getPartitionRetentionCondition();
        Assertions.assertEquals("date_trunc('day', k1) > current_date() - interval 2 month", retentionCondition);
        starRocksAssert.dropMaterializedView("mv1");
        starRocksAssert.dropTable("tt1");
    }

    @Test
    public void testDisableCreateListMVWithDateTimeRollup1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS test_tbl_A (\n" +
                "  hour DATETIME,\n" +
                "  partner_id BIGINT,\n" +
                "  impressions BIGINT\n" +
                ")PARTITION BY (hour);");
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.test_mv_A\n" +
                    "PARTITION BY day\n" +
                    "REFRESH ASYNC\n" +
                    "AS\n" +
                    "select\n" +
                    "    date_trunc('day', hour) as day,\n" +
                    "    partner_id,\n" +
                    "    sum(impressions) as impressions\n" +
                    "from test_tbl_A\n" +
                    "group by 1,2");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("List materialized view's partition expression can only refer " +
                    "ref-base-table's partition expression without transforms but contains"));
        }
    }

    @Test
    public void testDisableCreateListMVWithDateTimeRollup2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS test_tbl_A (\n" +
                "  hour DATETIME,\n" +
                "  partner_id BIGINT,\n" +
                "  impressions BIGINT\n" +
                ")PARTITION BY (partner_id, hour);");
        try {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.test_mv_A\n" +
                    "PARTITION BY (day, partner_id)\n" +
                    "REFRESH ASYNC\n" +
                    "AS\n" +
                    "select\n" +
                    "    date_trunc('day', hour) as day,\n" +
                    "    partner_id,\n" +
                    "    sum(impressions) as impressions\n" +
                    "from test_tbl_A\n" +
                    "group by 1,2");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("List materialized view's partition expression can only refer " +
                    "ref-base-table's partition expression without transforms but contains"));
        }
    }

    @Test
    public void testCreateMVWithWrongPartitionByExprs1() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tt1 (\n" +
                "        sku_id varchar(100),\n" +
                "        total_amount decimal,\n" +
                "        id int,\n" +
                "        create_time int\n" +
                ")\n" +
                "PARTITION BY RANGE(from_unixtime(create_time))(\n" +
                "START (\"2021-01-01\") END (\"2021-01-10\") EVERY (INTERVAL 1 DAY)\n" +
                ");");
        try {
            starRocksAssert.withMaterializedView("create materialized view mv1 refresh manual " +
                    "partition by create_time as select id,create_time from tt1;");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Materialized view partition function derived from " +
                    "CAST(from_unixtime(create_time) AS DATETIME) of base table tt1 is not supported yet"));
        }
    }

    @Test
    public void testCreateMVWithWrongPartitionByExprs2() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tt1 (\n" +
                "        sku_id varchar(100),\n" +
                "        total_amount decimal,\n" +
                "        id int,\n" +
                "        create_time string\n" +
                ")\n" +
                "PARTITION BY RANGE(str2date(create_time, '%Y-%m-%d'))(\n" +
                "START (\"2021-01-01\") END (\"2021-01-10\") EVERY (INTERVAL 1 DAY)\n" +
                ");");
        try {
            starRocksAssert.withMaterializedView("create materialized view mv1 refresh manual " +
                    "partition by create_time as select id,create_time from tt1;");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Materialized view is partitioned by string " +
                    "type column create_time but ref base table tt1 is range partitioned, " +
                    "please use str2date partition expression."));
        }
    }

    @Test
    public void testCreateMaterializedViewOnListPartitionTablesActive() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "partition by province " +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt, province, avg(age) from list_partition_tbl1 group by dt, province;";
        try {
            starRocksAssert.withMaterializedView(sql);
            MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "list_partition_mv1");

            String result = mv.getMaterializedViewDdlStmt(false, false);
            String expect = "CREATE MATERIALIZED VIEW `list_partition_mv1` (`dt`, `province`, `avg(age)`)\n" +
                    "PARTITION BY (`province`)\n" +
                    "DISTRIBUTED BY HASH(`dt`, `province`) BUCKETS 10 \n" +
                    "REFRESH MANUAL\n" +
                    "PROPERTIES (\n" +
                    "\"replicated_storage\" = \"true\",\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"storage_medium\" = \"HDD\"\n" +
                    ")\n" +
                    "AS SELECT `test`.`list_partition_tbl1`.`dt`, `test`.`list_partition_tbl1`.`province`, " +
                    "avg(`test`.`list_partition_tbl1`.`age`) AS `avg(age)`\n" +
                    "FROM `test`.`list_partition_tbl1`\n" +
                    "GROUP BY `test`.`list_partition_tbl1`.`dt`, `test`.`list_partition_tbl1`.`province`;";
            Assertions.assertTrue(expect.equals(result));

            sql = "alter materialized view list_partition_mv1 inactive";
            starRocksAssert.alterMvProperties(sql);

            sql = "alter materialized view list_partition_mv1 active";
            starRocksAssert.alterMvProperties(sql);

            mv = (MaterializedView) starRocksAssert.getTable("test", "list_partition_mv1");
            result = mv.getMaterializedViewDdlStmt(false, false);
            Assertions.assertTrue(expect.equals(result));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testCreateMaterializedViewOnMultiPartitionColumns1() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt datetime,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY province, date_trunc('day', dt) \n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "PARTITION BY (pr1, date_trunc('day', dt1)) \n" +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt as dt1, province as pr1, avg(age) from list_partition_tbl1 group by dt, province;";
        try {
            starRocksAssert.withMaterializedView(sql);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Please check the partition expression pr1, " +
                    "it should refer base table's partition column directly."));
        }
        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testCreateMaterializedViewOnMultiPartitionColumns2() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt datetime,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY province, date_trunc('day', dt) \n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "PARTITION BY (date_trunc('day', dt)) \n" +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt as dt, province , avg(age) from list_partition_tbl1 group by dt, province;";
        try {
            starRocksAssert.withMaterializedView(sql);
        } catch (Exception e) {
            Assertions.fail();
        }
        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testCreateMaterializedViewOnMultiPartitionColumns_MTON() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl_m_to_n (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt datetime,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY age, province, date_trunc('day', dt) \n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "PARTITION BY (province, date_trunc('day', dt)) \n" +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt as dt, province , avg(age) from list_partition_tbl_m_to_n group by dt, province;";
        try {
            starRocksAssert.withMaterializedView(sql);
        } catch (Exception e) {
            Assertions.fail();
        }
        starRocksAssert.dropTable("list_partition_tbl_m_to_n");
    }

    @Test
    public void testCreateMaterializedViewOnMultiPartitionColumnsActive1() throws Exception {
        String createSQL = "CREATE TABLE test.list_partition_tbl1 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt datetime,\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY province, date_trunc('day', dt) \n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ")";
        starRocksAssert.withTable(createSQL);

        String sql = "create materialized view list_partition_mv1 " +
                "PARTITION BY (province, date_trunc('day', dt)) \n" +
                "distributed by hash(dt, province) buckets 10 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"" +
                ") " +
                "as select dt, province, avg(age) from list_partition_tbl1 group by dt, province;";
        starRocksAssert.withMaterializedView(sql);
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable("test", "list_partition_mv1");

        String result = mv.getMaterializedViewDdlStmt(false, false);
        System.out.println(result);

        sql = "alter materialized view list_partition_mv1 inactive";
        starRocksAssert.alterMvProperties(sql);

        sql = "alter materialized view list_partition_mv1 active";
        starRocksAssert.alterMvProperties(sql);

        mv = (MaterializedView) starRocksAssert.getTable("test", "list_partition_mv1");
        String result2 = mv.getMaterializedViewDdlStmt(false, false);
        Assertions.assertTrue(result2.equals(result));

        starRocksAssert.dropTable("list_partition_tbl1");
    }

    @Test
    public void testRefreshMVWithExternalTable1() throws Exception {
        new MockUp<MaterializedViewAnalyzer>() {
            @Mock
            public static boolean isExternalTableFromResource(Table t) {
                return true;
            }
        };
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        starRocksAssert.withMaterializedView(sql);
        starRocksAssert.refreshMV(connectContext, "mv1");
    }

    @Test
    public void testRefreshMVWithExternalTable2() throws Exception {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) buckets 10 " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")" +
                "as select tbl1.k1 ss, tbl1.k2 from mysql_external_table tbl1;";
        starRocksAssert.withMaterializedView(sql);
        starRocksAssert.refreshMV(connectContext, "mv1");
    }

    @Test
    public void testAdaptiveRefreshMVWithExternalTable1() throws Exception {
        String sql = "create materialized view mv_table_with_external_table " +
                "partition by str2date(d,'%Y-%m-%d') " +
                "distributed by hash(a) " +
                "REFRESH DEFERRED MANUAL " +
                "PROPERTIES (\n" +
                "'replication_num' = '1',\n" +
                "'partition_refresh_strategy' = 'adaptive'" +
                ") \n" +
                "as select a, b, d, bitmap_union(to_bitmap(t1.c))" +
                " from iceberg0.partitioned_db.part_tbl1 as t1 " +
                " group by a, b, d;";
        starRocksAssert.withMaterializedView(sql);
        starRocksAssert.refreshMV(connectContext, "mv_table_with_external_table");
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }

    @BeforeEach
    public void setup(TestInfo testInfo) {
        Optional<Method> testMethod = testInfo.getTestMethod();
        if (testMethod.isPresent()) {
            this.name = testMethod.get().getName();
        }
    }
}