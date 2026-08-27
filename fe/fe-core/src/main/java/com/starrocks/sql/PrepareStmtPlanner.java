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

package com.starrocks.sql;

import com.starrocks.common.VectorSearchOptions;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.optimizer.rule.transformation.RewriteToVectorPlanRule;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;

import java.util.ArrayList;
import java.util.List;

public class PrepareStmtPlanner {

    public static ExecPlan plan(ExecuteStmt executeStmt, StatementBase stmt, ConnectContext session) {
        try {
            if (!(stmt instanceof QueryStatement)) {
                return StatementPlanner.plan(stmt, session);
            }
            QueryStatement queryStmt = (QueryStatement) stmt;
            PrepareStmtContext prepareStmtContext = session.getPreparedStmt(executeStmt.getStmtName());
            if (!queryStmt.isPointQuery()) {
                if (session.getSessionVariable().isEnableVectorSearchPlanCache()) {
                    return planVectorQuery(executeStmt, queryStmt, session, prepareStmtContext);
                }
                return StatementPlanner.plan(stmt, session);
            }

            if (!prepareStmtContext.isCached()) {
                return planAndCacheExecPlan(stmt, session, prepareStmtContext);
            } else {
                if (prepareStmtContext.needReAnalyze(queryStmt, session)) {
                    return planAndCacheExecPlan(stmt, session, prepareStmtContext);
                } else {
                    ExecPlan execPlan = prepareStmtContext.getExecPlan();

                    // use cache and rebuild physical plan
                    rePlan(executeStmt, execPlan.getLogicalPlan(), execPlan.getPhysicalPlan());

                    TResultSinkType resultSinkType = session instanceof HttpConnectContext ? TResultSinkType.HTTP_PROTOCAL :
                            TResultSinkType.MYSQL_PROTOCAL;
                    resultSinkType = queryStmt.hasOutFileClause() ? TResultSinkType.FILE : resultSinkType;

                    OptExpression physicalPlan = execPlan.getPhysicalPlan();
                    LogicalPlan logicalPlan = execPlan.getLogicalPlan();
                    ColumnRefFactory columnRefFactory = execPlan.getColumnRefFactory();
                    QueryRelation query = queryStmt.getQueryRelation();
                    List<String> colNames = query.getColumnOutputNames();

                    return PlanFragmentBuilder.createPhysicalPlan(
                            physicalPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                            colNames,
                            resultSinkType,
                            !session.getSessionVariable().isSingleNodeExecPlan());
                }
            }
        } finally {
            // Release query-level connector metadata when planning is done
            GlobalStateMgr.getCurrentState().getMetadataMgr().removeQueryMetadata();
        }
    }

    private static ExecPlan planVectorQuery(ExecuteStmt executeStmt, QueryStatement queryStmt,
                                            ConnectContext session, PrepareStmtContext prepareStmtContext) {
        if (!prepareStmtContext.isCached()) {
            return planAndMaybeCacheVectorQuery(queryStmt, session, prepareStmtContext);
        }

        ExecPlan cachedPlan = prepareStmtContext.getExecPlan();
        PhysicalOlapScanOperator vectorScan = findCacheableVectorScan(cachedPlan.getPhysicalPlan());
        if (vectorScan == null) {
            return StatementPlanner.plan(queryStmt, session);
        }

        if (prepareStmtContext.needReAnalyze(queryStmt, session)) {
            prepareStmtContext.reset();
            return planAndMaybeCacheVectorQuery(queryStmt, session, prepareStmtContext);
        }

        try (Timer ignored = Tracers.watchScope("VectorPlanCacheRebind")) {
            if (!rebindVectorQuery(executeStmt, vectorScan.getVectorSearchOptions())) {
                prepareStmtContext.reset();
                return StatementPlanner.plan(queryStmt, session);
            }
        }

        Tracers.record("VectorPlanCache", "HIT");
        return rebuildExecPlan(queryStmt, session, cachedPlan);
    }

    private static ExecPlan planAndMaybeCacheVectorQuery(QueryStatement queryStmt, ConnectContext session,
                                                         PrepareStmtContext prepareStmtContext) {
        ExecPlan execPlan = StatementPlanner.plan(queryStmt, session);
        if (execPlan != null && findCacheableVectorScan(execPlan.getPhysicalPlan()) != null) {
            prepareStmtContext.setExecPlan(execPlan);
            prepareStmtContext.updateLastSchemaUpdateTime(queryStmt, session);
            prepareStmtContext.cachePlan(execPlan);
            Tracers.record("VectorPlanCache", "MISS_CACHED");
        } else {
            Tracers.record("VectorPlanCache", "MISS_NOT_CACHEABLE");
        }
        return execPlan;
    }

    private static PhysicalOlapScanOperator findCacheableVectorScan(OptExpression expression) {
        List<PhysicalOlapScanOperator> scans = new ArrayList<>();
        collectPhysicalOlapScans(expression, scans);
        if (scans.size() != 1) {
            return null;
        }

        PhysicalOlapScanOperator scan = scans.get(0);
        VectorSearchOptions options = scan.getVectorSearchOptions();
        if (options == null || !options.isEnableUseANN() || options.isRefineDistance() ||
                scan.getPredicate() != null) {
            return null;
        }
        return scan;
    }

    private static void collectPhysicalOlapScans(OptExpression expression, List<PhysicalOlapScanOperator> scans) {
        if (expression == null) {
            return;
        }
        if (expression.getOp() instanceof PhysicalOlapScanOperator) {
            scans.add((PhysicalOlapScanOperator) expression.getOp());
        }
        for (OptExpression child : expression.getInputs()) {
            collectPhysicalOlapScans(child, scans);
        }
    }

    private static boolean rebindVectorQuery(ExecuteStmt executeStmt, VectorSearchOptions options) {
        List<Expr> params = executeStmt.getParamsExpr();
        if (params == null || params.size() != 1 || !(params.get(0) instanceof LiteralExpr)) {
            return false;
        }
        String literal = ((LiteralExpr) params.get(0)).getStringValue();
        List<String> queryVector = RewriteToVectorPlanRule.parseVectorLiteral(literal);
        if (queryVector.size() != options.getQueryVectorSize()) {
            return false;
        }
        options.setQueryVector(queryVector);
        return true;
    }

    private static ExecPlan rebuildExecPlan(QueryStatement queryStmt, ConnectContext session, ExecPlan cachedPlan) {
        TResultSinkType resultSinkType = session instanceof HttpConnectContext ? TResultSinkType.HTTP_PROTOCAL :
                TResultSinkType.MYSQL_PROTOCAL;
        resultSinkType = queryStmt.hasOutFileClause() ? TResultSinkType.FILE : resultSinkType;

        QueryRelation query = queryStmt.getQueryRelation();
        try (Timer ignored = Tracers.watchScope("ExecPlanBuild")) {
            return PlanFragmentBuilder.createPhysicalPlan(
                    cachedPlan.getPhysicalPlan(), session, cachedPlan.getLogicalPlan().getOutputColumn(),
                    cachedPlan.getColumnRefFactory(), query.getColumnOutputNames(), resultSinkType,
                    !session.getSessionVariable().isSingleNodeExecPlan());
        }
    }

    private static ExecPlan planAndCacheExecPlan(StatementBase stmt, ConnectContext session,
                                                 PrepareStmtContext prepareStmtContext) {
        ExecPlan execPlan = StatementPlanner.plan(stmt, session);
        if (execPlan == null) {
            return null;
        }

        prepareStmtContext.setExecPlan(execPlan);
        prepareStmtContext.updateLastSchemaUpdateTime((QueryStatement) stmt, session);
        prepareStmtContext.cachePlan(execPlan);
        return execPlan;
    }

    private static void rePlan(ExecuteStmt executeStmt,
                               LogicalPlan logicalPlan,
                               OptExpression optimizedPlan) {

        Operator operator = logicalPlan.getRoot().getInputs().get(0).getOp();
        if (operator instanceof LogicalFilterOperator) {
            ScalarOperator.updateLiteralPredicates(operator.getPredicate(), executeStmt.getParamsExpr());
        }

        rePlanOptimizedPlan(logicalPlan, optimizedPlan);
    }

    private static void rePlanOptimizedPlan(LogicalPlan logicalPlan, OptExpression optimizedPlan) {
        if (!(optimizedPlan.getOp() instanceof PhysicalOlapScanOperator)) {
            return;
        }

        ScalarOperator predicate = logicalPlan.getRoot().getInputs().get(0).getOp().getPredicate();

        // process logical scan operator
        LogicalOlapScanOperator logicalScanOperator =
                (LogicalOlapScanOperator) logicalPlan.getRoot().getInputs().get(0).getInputs().get(0)
                        .getInputs().get(0).getOp();
        LogicalOlapScanOperator logicalOlapScanOperator =
                OptOlapPartitionPruner.prunePartitions(logicalScanOperator);
        logicalOlapScanOperator
                .buildColumnFilters(predicate);

        // update optimized plan partitionIds and tabletIds with predicates
        optimizedPlan.getOp().setPredicate(predicate);
        PhysicalOlapScanOperator physicalOlapScanOperator = (PhysicalOlapScanOperator) optimizedPlan.getOp();
        physicalOlapScanOperator.setSelectedPartitionId(logicalOlapScanOperator.getSelectedPartitionId());
        List<Long> pruneTabletIds = OptDistributionPruner.pruneTabletIds(logicalOlapScanOperator,
                logicalOlapScanOperator.getSelectedPartitionId());

        physicalOlapScanOperator.setSelectedTabletId(pruneTabletIds);
    }

}
