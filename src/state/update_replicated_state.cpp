//---------------------------------------------------------------------------//
// Copyright (c) 2018-2022 Mikhail Komarov <nemo@nil.foundation>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the Server Side Public License, version 1,
// as published by the author.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// Server Side Public License for more details.
//
// You should have received a copy of the Server Side Public License
// along with this program. If not, see
// <https://github.com/NilFoundation/dbms/blob/master/LICENSE_1_0.txt>.
//---------------------------------------------------------------------------//

#include "logger/LogMacros.h"
#include <nil/dbms/replication/state/update_state.hpp>
#include <nil/dbms/replication/state/state_common.hpp>
#include "basics/voc_errors.h"
#include "logger/LogContextKeys.h"

using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;

auto algorithms::updateReplicatedState(StateActionContext &ctx, std::string const &serverId, log_id id,
                                       state::agency::Plan const *spec,
                                       state::agency::Current const *current) -> nil::dbms::Result {
    if (spec == nullptr) {
        return ctx.dropReplicatedState(id);
    }

    TRI_ASSERT(id == spec->id);
    TRI_ASSERT(spec->participants.find(serverId) != spec->participants.end());
    auto expectedGeneration = spec->participants.at(serverId).generation;

    auto logCtx = logger_context(Logger::REPLICATED_STATE).with<logContextKeylog_id>(id);

    LOG_CTX("b089c", TRACE, logCtx) << "Update replicated log" << id << " for generation " << expectedGeneration;

    auto state = ctx.getReplicatedStateById(id);
    if (state == nullptr) {
        // TODO use user data instead of non-slice
        auto result =
            ctx.create_replicated_state(id, spec->properties.implementation.type, velocypack::Slice::noneSlice());
        if (result.fail()) {
            return result.result();
        }

        state = result.get();

        auto token = std::invoke([&] {
            if (current) {
                if (auto const p = current->participants.find(serverId); p != std::end(current->participants)) {
                    if (p->second.generation == expectedGeneration) {
                        LOG_CTX("19d00", DEBUG, logCtx) << "Using existing snapshot information from current";
                        // we are allowed to use the information stored here
                        return std::make_unique<ReplicatedStateToken>(
                            ReplicatedStateToken::withExplicitSnapshotStatus(expectedGeneration, p->second.snapshot));
                    } else {
                        LOG_CTX("6d8c9", DEBUG, logCtx)
                            << "Must not use existing information, generation is "
                               "different. "
                            << "Plan = " << expectedGeneration << " but Current = " << p->second.generation;
                    }
                } else {
                    LOG_CTX("cef1a", DEBUG, logCtx) << "No snapshot information available for this server " << serverId;
                }
            } else {
                LOG_CTX("d4fd8", DEBUG, logCtx) << "no current available to read snapshot information from. "
                                                   "Assuming no snapshot available";
            }

            return std::make_unique<ReplicatedStateToken>(expectedGeneration);
        });
        // now start the replicated state
        state->start(std::move(token));
        return {TRI_ERROR_NO_ERROR};
    } else {
        auto status = state->get_status();
        if (status.has_value()) {
            auto generation = status.value().getGeneration();
            if (generation != expectedGeneration) {
                state->flush(expectedGeneration);
            }
        }
        return {TRI_ERROR_NO_ERROR};
    }
}
