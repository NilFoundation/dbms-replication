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
#include <nil/dbms/cluster/cluster_types.hpp>

#include "velocypack/Builder.h"
#include "velocypack/velocypack-common.h"

#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/log_common.hpp>
#include <nil/dbms/replication/log/participants_health.hpp>
#include <nil/dbms/replication/log/supervision_action.hpp>
#include <nil/dbms/replication/log/supervision_context.hpp>

using namespace nil::dbms::replication::agency;

namespace nil::dbms::replication::log {

    using log_current_local_states = std::unordered_map<ParticipantId, log_current_local_state>;

    auto isLeaderFailed(log_plan_term_specification::Leader const &leader, ParticipantsHealth const &health) -> bool;

    auto computeReason(std::optional<log_current_local_state> const &maybeStatus, bool healthy, bool excluded,
                       log_term term) -> log_current_supervision_election::ErrorCode;

    auto runElectionCampaign(log_current_local_states const &states, participants_config const &participants_config,
                             ParticipantsHealth const &health, log_term term) -> log_current_supervision_election;

    auto getParticipantsAcceptableAsLeaders(ParticipantId const &currentLeader,
                                            ParticipantsFlagsMap const &participants) -> std::vector<ParticipantId>;

    // Actions capture entries in log, so they have to stay
    // valid until the returned action has been executed (or discarded)
    auto checkReplicatedLog(SupervisionContext &ctx, Log const &log, ParticipantsHealth const &health) -> void;

    auto executeCheckReplicatedLog(DatabaseID const &database, std::string const &log_idString, Log log,
                                   ParticipantsHealth const &health, nil::dbms::agency::envelope envelope) noexcept
        -> nil::dbms::agency::envelope;

    auto build_agency_transaction(DatabaseID const &dbName, log_id const &log_id, SupervisionContext &sctx,
                                ActionContext &actx, size_t maxActionsTraceLength, nil::dbms::agency::envelope envelope)
        -> nil::dbms::agency::envelope;

}    // namespace nil::dbms::replication::log
