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

#include "paimon_table_sink.h"

#include "exec/pipeline/sink/paimon_table_sink_operator.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

PaimonTableSink::PaimonTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

PaimonTableSink::~PaimonTableSink() = default;

Status PaimonTableSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(thrift_sink, state));
    RETURN_IF_ERROR(prepare(state));
    RETURN_IF_ERROR(open(state));
    return Status::OK();
}

Status PaimonTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "PaimonTableSink (frag_id=" << state->fragment_instance_id() << ")";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status PaimonTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    return Status::OK();
}

Status PaimonTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::OK();
}

Status PaimonTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}

Status PaimonTableSink::decompose_to_pipeline(pipeline::OpFactories prev_operators, const TDataSink& thrift_sink,
                                              pipeline::PipelineBuilderContext* context) const {
    auto* runtime_state = context->runtime_state();
    auto* fragment_ctx = context->fragment_context();
    TableDescriptor* table_desc =
            runtime_state->desc_tbl().get_table_descriptor(thrift_sink.paimon_table_sink.target_table_id);
    const auto& t_paimon_sink = thrift_sink.paimon_table_sink;
    auto column_types = t_paimon_sink.data_column_types;

    auto* paimon_table_desc = down_cast<PaimonTableDescriptor*>(table_desc);

    DCHECK(thrift_sink.paimon_table_sink.__isset.target_table_id);
    DCHECK(thrift_sink.paimon_table_sink.__isset.cloud_configuration);

    std::vector<TExpr> partition_exprs;
    std::vector<std::string> partition_column_names;

    std::vector<ExprContext*> partition_expr_ctxs;
    RETURN_IF_ERROR(
            Expr::create_expr_trees(runtime_state->obj_pool(), partition_exprs, &partition_expr_ctxs, runtime_state));

    std::vector<ExprContext*> output_expr_ctxs;
    auto output_exprs = this->get_output_expr();
    RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state->obj_pool(), output_exprs, &output_expr_ctxs, runtime_state));

    auto op = std::make_shared<pipeline::PaimonTableSinkOperatorFactory>(
            context->next_operator_id(), fragment_ctx, paimon_table_desc, thrift_sink.paimon_table_sink, output_exprs,
            partition_expr_ctxs, output_expr_ctxs, column_types);

    size_t sink_dop = context->data_sink_dop();

    if (t_paimon_sink.is_static_partition_sink || partition_expr_ctxs.empty()) {
        auto ops = context->maybe_interpolate_local_passthrough_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators, sink_dop,
                false);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    } else {
        auto ops = context->interpolate_local_key_partition_exchange(
                runtime_state, pipeline::Operator::s_pseudo_plan_node_id_for_final_sink, prev_operators,
                partition_expr_ctxs, sink_dop);
        ops.emplace_back(std::move(op));
        context->add_pipeline(std::move(ops));
    }

    return Status::OK();
}

} // namespace starrocks