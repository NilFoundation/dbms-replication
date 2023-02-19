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

#include <nil/dbms/replication/supervision/modify_context.hpp>
#include <nil/dbms/replication/replicated_log/agency_log_specification.hpp>
#include "agency_specification.hpp"
#include "state_common.hpp"

namespace nil::dbms::replication::replicated_state {

    using ActionContext = modify_context<replication::agency::log_target, agency::Plan, agency::Current::Supervision>;

    struct empty_action {
        void execute(ActionContext &) {
        }
    };

    struct add_participant_action {
        ParticipantId participant;

        void execute(ActionContext &ctx);
    };

    struct remove_participant_from_log_target_action {
        ParticipantId participant;

        void execute(ActionContext &ctx);
    };

    struct remove_participant_from_state_plan_action {
        ParticipantId participant;

        void execute(ActionContext &ctx);
    };

    struct add_state_to_plan_action {
        replication::agency::log_target logTarget;
        agency::Plan statePlan;

        void execute(ActionContext &ctx);
    };

    struct update_participant_flags_action {
        ParticipantId participant;
        participant_flags flags;

        void execute(ActionContext &ctx);
    };

    struct current_converged_action {
        std::uint64_t version;

        void execute(ActionContext &ctx);
    };

    struct set_leader_action {
        std::optional<ParticipantId> leader;

        void execute(ActionContext &ctx);
    };

    using Action = std::variant<empty_action, add_participant_action, remove_participant_from_log_target_action,
                                remove_participant_from_state_plan_action, add_state_to_plan_action,
                                update_participant_flags_action, current_converged_action, set_leader_action>;

    auto executeAction(nil::dbms::replication::replicated_state::agency::State state,
                       std::optional<replication::agency::Log> log, Action &action) -> ActionContext;

}    // namespace nil::dbms::replication::replicated_state
