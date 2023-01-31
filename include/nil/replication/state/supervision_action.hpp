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

#pragma once
#include <memory>
#include <utility>

#include <nil/dbms/agency/transaction_builder.hpp>
#include <nil/dbms/replication/supervision/modify_context.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/state/agency_specification.hpp>
#include <nil/dbms/replication/state/state_common.hpp>

namespace nil::dbms::replication::state {

    using ActionContext = ModifyContext<replication::agency::LogTarget, agency::Plan, agency::Current::Supervision>;

    struct EmptyAction {
        void execute(ActionContext &) {
        }
    };

    struct AddParticipantAction {
        ParticipantId participant;

        void execute(ActionContext &ctx) {
            ctx.modify<agency::Plan, replication::agency::LogTarget>([&](auto &plan, auto &logTarget) {
                logTarget.participants[participant] =
                    ParticipantFlags {.allowedInQuorum = false, .allowedAsLeader = false};

                plan.participants[participant].generation = plan.generation;
                plan.generation.value += 1;
            });
        }
    };

    struct RemoveParticipantFromLogTargetAction {
        ParticipantId participant;

        void execute(ActionContext &ctx) {
            ctx.modify<agency::Plan, replication::agency::LogTarget>(
                [&](auto &plan, auto &logTarget) { logTarget.participants.erase(participant); });
        }
    };

    struct RemoveParticipantFromStatePlanAction {
        ParticipantId participant;

        void execute(ActionContext &ctx) {
            ctx.modify<agency::Plan, replication::agency::LogTarget>(
                [&](auto &plan, auto &logTarget) { plan.participants.erase(participant); });
        }
    };

    struct AddStateToPlanAction {
        replication::agency::LogTarget logTarget;
        agency::Plan statePlan;

        void execute(ActionContext &ctx) {
            ctx.setValue<agency::Plan>(std::move(statePlan));
            ctx.setValue<replication::agency::LogTarget>(std::move(logTarget));
        }
    };

    struct UpdateParticipantFlagsAction {
        ParticipantId participant;
        ParticipantFlags flags;

        void execute(ActionContext &ctx) {
            ctx.modify<replication::agency::LogTarget>(
                [&](auto &target) { target.participants.at(participant) = flags; });
        }
    };

    struct CurrentConvergedAction {
        std::uint64_t version;

        void execute(ActionContext &ctx) {
            ctx.modify_or_create<state::agency::Current::Supervision>(
                [&](auto &current) { current.version = version; });
        }
    };

    struct SetLeaderAction {
        std::optional<ParticipantId> leader;

        void execute(ActionContext &ctx) {
            ctx.modify<replication::agency::LogTarget>([&](auto &target) { target.leader = leader; });
        }
    };

    using Action = std::variant<EmptyAction, AddParticipantAction, RemoveParticipantFromLogTargetAction,
                                RemoveParticipantFromStatePlanAction, AddStateToPlanAction,
                                UpdateParticipantFlagsAction, CurrentConvergedAction, SetLeaderAction>;

    auto executeAction(nil::dbms::replication::state::agency::State state,
                       std::optional<replication::agency::Log> log, Action &action) -> ActionContext;

}    // namespace nil::dbms::replication::state
