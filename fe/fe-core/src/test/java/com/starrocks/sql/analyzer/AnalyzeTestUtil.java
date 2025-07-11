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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;

public class AnalyzeTestUtil {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    protected static String DB_NAME = "test";

    public static void initWithoutTableAndDb(RunMode runMode) throws Exception {
        Config.enable_experimental_rowstore = true;
        // create connect context
        if (runMode == RunMode.SHARED_DATA) {
            UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        } else {
            UtFrameUtils.createMinStarRocksCluster();
        }
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
    }

    public static void init() throws Exception {
        Config.enable_experimental_rowstore = true;
        // create connect context
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t2` (\n" +
                "  `v7` bigint NULL COMMENT \"\",\n" +
                "  `v8` bigint NULL COMMENT \"\",\n" +
                "  `v9` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v7`, `v8`, v9)\n" +
                "DISTRIBUTED BY HASH(`v7`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t3` (\n" +
                "  `v10` bigint NULL COMMENT \"\",\n" +
                "  `v11` bigint NULL COMMENT \"\",\n" +
                "  `v12` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v10`, `v11`, v12)\n" +
                "DISTRIBUTED BY HASH(`v10`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `T3` (\n" +
                "  `v10` bigint NULL COMMENT \"\",\n" +
                "  `v11` bigint NULL COMMENT \"\",\n" +
                "  `v12` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v10`, `v11`, v12)\n" +
                "DISTRIBUTED BY HASH(`v10`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tall` (\n" +
                "  `ta` varchar(20) NULL COMMENT \"\",\n" +
                "  `tb` smallint(6) NULL COMMENT \"\",\n" +
                "  `tc` int(11) NULL COMMENT \"\",\n" +
                "  `td` bigint(20) NULL COMMENT \"\",\n" +
                "  `te` float NULL COMMENT \"\",\n" +
                "  `tf` double NULL COMMENT \"\",\n" +
                "  `tg` bigint(20) NULL COMMENT \"\",\n" +
                "  `th` datetime NULL COMMENT \"\",\n" +
                "  `ti` date NULL COMMENT \"\",\n" +
                "  `tj` decimal(9, 3) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ta`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ta`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `test_object` (\n" +
                "  `v1` int(11) NULL,\n" +
                "  `v2` int(11) NULL,\n" +
                "  `v3` int(11) NULL,\n" +
                "  `v4` int(11) NULL,\n" +
                "  `b1` bitmap BITMAP_UNION NULL,\n" +
                "  `b2` bitmap BITMAP_UNION NULL,\n" +
                "  `b3` bitmap BITMAP_UNION NULL,\n" +
                "  `b4` bitmap BITMAP_UNION NULL,\n" +
                "  `h1` hll hll_union NULL,\n" +
                "  `h2` hll hll_union NULL,\n" +
                "  `h3` hll hll_union NULL,\n" +
                "  `h4` hll hll_union NULL,\n" +
                "  `p1` percentile PERCENTILE_UNION NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`v1`, `v2`, `v3`, `v4`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tarray` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` ARRAY<bigint(20)>  NULL,\n" +
                "  `v4` ARRAY<largeint>  NULL,\n" +
                "  `v5` ARRAY<json>  NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tnotnull` (\n" +
                "  `v1` bigint NOT NULL,\n" +
                "  `v2` bigint NOT NULL,\n" +
                "  `v3` bigint NOT NULL DEFAULT \"100\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tjson` (\n" +
                "  `v_int`  bigint NULL COMMENT \"\",\n" +
                "  `v_json` json NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v_int`)\n" +
                "DISTRIBUTED BY HASH(`v_int`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tprimary` (\n" +
                "  `pk` bigint NOT NULL COMMENT \"\",\n" +
                "  `v1` string NOT NULL COMMENT \"\",\n" +
                "  `v2` int NOT NULL,\n" +
                "  `v4` int NOT NULL,\n" +
                "  `v3` array<int> not null" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tprimary2` (\n" +
                "  `pk` bigint NOT NULL COMMENT \"\",\n" +
                "  `v1` string NOT NULL COMMENT \"\",\n" +
                "  `v2` int NOT NULL,\n" +
                "  `v3` array<int> not null" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `ttypes` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `vm` map<bigint(20), char(20)>  NULL,\n" +
                "  `vm1` map<bigint(20), char(20)>  NULL,\n" +
                "  `va` array<bigint(20)>  NULL,\n" +
                "  `va1` array<bigint(20)>  NULL,\n" +
                "  `vs` struct<a bigint(20), b char(20)>  NULL,\n" +
                "  `vs1` struct<a bigint(20), b char(20)>  NULL,\n" +
                "  `vj` json  NULL,\n" +
                "  `vj1` json  NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable(
                "create table tp(c1 int, c2 int, c3 int) DUPLICATE KEY(c1, c2) PARTITION BY RANGE(c1) "
                        + "(PARTITION p1 VALUES [('-2147483648'), ('10')), PARTITION p2 VALUES [('10'), ('20')))"
                        + " DISTRIBUTED BY HASH(`c2`) BUCKETS 2 PROPERTIES('replication_num'='1');");
        starRocksAssert.withTable("CREATE TABLE test.table_to_drop\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withView("create view test.view_to_drop as select * from test.table_to_drop;");

        starRocksAssert.withDatabase("db1");
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable("CREATE TABLE db1.`t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE db1.`t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE db2.`t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        // varbinary table
        starRocksAssert.withTable("CREATE TABLE `tbinary` (\n" +
                "  `v_int`  bigint NULL COMMENT \"\",\n" +
                "  `v_varbinary4`  varbinary(4) NULL COMMENT \"\",\n" +
                "  `v_varbinary` varbinary NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v_int`)\n" +
                "DISTRIBUTED BY HASH(`v_int`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tmc` (\n" +
                "  `id`  bigint NULL COMMENT \"\",\n" +
                "  `name`  bigint NULL COMMENT \"\",\n" +
                "  `mc` bigint NULL AS (id + name) COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tmcwr` (\n" +
                "  `id`  bigint COMMENT \"\",\n" +
                "  `name`  bigint NULL COMMENT \"\",\n" +
                "  `mc` bigint NULL COMMENT \"\" ,\n" +
                "  `ff` bigint NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_type\" = \"column_with_row\"" +
                ");");
        starRocksAssert.withTable("CREATE TABLE test.auto_tbl1 (\n" +
                "  col1 varchar(100),\n" +
                "  col2 varchar(100),\n" +
                "  col3 bigint\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY (col1)\n" +
                "PARTITION BY (col1)\n" +
                "DISTRIBUTED BY HASH(col1) BUCKETS 5\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE test.test_exclude ( \n" + 
                " id INT, \n" +
                " name VARCHAR(50), \n" +
                " age INT, \n" +
                " email VARCHAR(100)) \n" +
                " DUPLICATE KEY(id) PROPERTIES ( \n" +
                "\"replication_num\" = \"1\"\n" +
                ");");
    }

    public static String getDbName() {
        return DB_NAME;
    }

    public static ConnectContext getConnectContext() {
        return connectContext;
    }

    public static void setConnectContext(ConnectContext ctx) {
        connectContext = ctx;
    }

    public static StarRocksAssert getStarRocksAssert() {
        return starRocksAssert;
    }

    public static StatementBase parseSql(String originStmt) {
        return com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable()).get(0);
    }

    public static StatementBase analyzeSuccess(String originStmt) {
        try {
            StatementBase statementBase = parseSql(originStmt);
            Analyzer.analyze(statementBase, connectContext);

            if (statementBase instanceof QueryStatement) {
                StatementBase viewStatement =
                        com.starrocks.sql.parser.SqlParser.parse(AstToSQLBuilder.toSQL(statementBase),
                                connectContext.getSessionVariable()).get(0);
                Analyzer.analyze(viewStatement, connectContext);
            }

            return statementBase;
        } catch (Exception ex) {
            ex.printStackTrace();
            Assertions.fail();
            throw ex;
        }
    }

    public static StatementBase analyzeWithoutTestView(String originStmt) {
        try {
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(originStmt,
                    connectContext.getSessionVariable()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            return statementBase;
        } catch (Exception ex) {
            ex.printStackTrace();
            Assertions.fail();
            return null;
        }
    }

    public static void analyzeFail(String originStmt) {
        analyzeFail(originStmt, "");
    }

    public static void analyzeFail(String originStmt, String exceptMessage) {
        try {
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(originStmt,
                    connectContext.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            Assertions.fail("Miss semantic error exception");
        } catch (ParsingException | SemanticException | UnsupportedException | ErrorReportException e) {
            if (!exceptMessage.equals("")) {
                Assertions.assertTrue(e.getMessage().contains(exceptMessage), e.getMessage());
            }
        }
    }

    public static void analyzeSetUserVariableFail(String originStmt, String exceptMessage) {
        try {
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(originStmt,
                    connectContext.getSessionVariable().getSqlMode()).get(0);
            Analyzer.analyze(statementBase, connectContext);
            SetStmt setStmt = (SetStmt) statementBase;
            SetStmtAnalyzer.calcuteUserVariable((UserVariable) setStmt.getSetListItems().get(0));
            Assertions.fail("Miss semantic error exception");
        } catch (ParsingException | SemanticException | UnsupportedException e) {
            if (!exceptMessage.equals("")) {
                Assertions.assertTrue(e.getMessage().contains(exceptMessage), e.getMessage());
            }
        } catch (Exception e) {
            Assertions.fail("analyze exception: " + e);
        }
    }
}
