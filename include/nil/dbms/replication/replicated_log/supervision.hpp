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

#include "basics/error_code.h"

#include "velocypack/Builder.h"
#include "velocypack/velocypack-common.h"

#include "agency_log_specification.hpp"
#include "log_common.hpp"
#include "participants_health.hpp"
#include "supervision_action.hpp"
#include "supervision_context.hpp"

using namespace nil::dbms::replication::agency;

namespace nil::dbms::replication::replicated_log {

    using LogCurrentLocalStates = std::unordered_map<ParticipantId, log_current_local_state>;

    auto is_leader_failed(log_plan_term_specification::Leader const &leader, participants_health const &health) -> bool;

    auto compute_reason(std::optional<log_current_local_state> const &maybeStatus, bool healthy, bool excluded,
                        log_term term) -> log_current_supervision_election::ErrorCode;

    auto run_election_campaign(LogCurrentLocalStates const &states, participants_config const &participantsConfig,
                               participants_health const &health, log_term term) -> log_current_supervision_election;

    auto get_participants_acceptable_as_leaders(ParticipantId const &currentLeader,
                                                ParticipantsFlagsMap const &participants) -> std::vector<ParticipantId>;

    // Actions capture entries in log, so they have to stay
    // valid until the returned action has been executed (or discarded)
    auto checkReplicatedLog(supervision_context &ctx, Log const &log, participants_health const &health) -> void;

    auto executeCheckReplicatedLog(DatabaseID const &database, std::string const &logIdString, Log log,
                                   participants_health const &health, nil::dbms::agency::envelope envelope) noexcept
        -> nil::dbms::agency::envelope;

    auto build_agency_transaction(DatabaseID const &dbName, LogId const &logId, supervision_context &sctx,
                                  ActionContext &actx, size_t maxActionsTraceLength,
                                  nil::dbms::agency::envelope envelope) -> nil::dbms::agency::envelope;

}    // namespace nil::dbms::replication::replicated_log
