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
#include <basics/result_t.h>
#include <nil/dbms/replication/cluster/cluster_types.hpp>
#include <nil/dbms/replication/replicated_log/agency_log_specification.hpp>
#include <nil/dbms/replication/replicated_state/agency_specification.hpp>
#include <optional>

#include "futures/Future.h"

namespace nil::dbms {
    class Result;
}    // namespace nil::dbms
namespace nil::dbms::replication {
    class LogId;
    struct log_term;
}    // namespace nil::dbms::replication
namespace nil::dbms::replication::agency {
    struct log_current_supervision;
    struct log_current_supervision_election;
    struct log_plan_specification;
    struct log_plan_term_specification;

}    // namespace nil::dbms::replication::agency

namespace nil::dbms::agency {
    struct envelope;

}    // namespace nil::dbms::agency

struct TRI_vocbase_t;

namespace nil::dbms::replication::agency::methods {

    auto update_term_specification_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, LogId id,
                                       log_plan_term_specification const &spec, std::optional<log_term> prevTerm = {})
        -> nil::dbms::agency::envelope;

    auto update_participants_config_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, LogId id,
                                        participants_config const &participantsConfig,
                                        participants_config const &prevConfig) -> nil::dbms::agency::envelope;

    auto update_term_specification(DatabaseID const &database, LogId id, log_plan_term_specification const &spec,
                                   std::optional<log_term> prevTerm = {}) -> futures::Future<ResultT<uint64_t>>;

    auto update_election_result(nil::dbms::agency::envelope envelope, DatabaseID const &database, LogId id,
                                log_current_supervision_election const &result) -> nil::dbms::agency::envelope;
    auto remove_election_result(nil::dbms::agency::envelope envelope, DatabaseID const &database, LogId id)
        -> nil::dbms::agency::envelope;

    auto delete_replicated_log_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, LogId id)
        -> nil::dbms::agency::envelope;
    auto delete_replicated_log(DatabaseID const &database, LogId id) -> futures::Future<ResultT<uint64_t>>;

    auto create_replicated_log_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database,
                                   log_target const &spec) -> nil::dbms::agency::envelope;
    auto create_replicated_log(DatabaseID const &database, log_target const &spec)
        -> futures::Future<ResultT<uint64_t>>;
    auto create_replicated_state(DatabaseID const &database, replicated_state::agency::Target const &spec)
        -> futures::Future<ResultT<uint64_t>>;
    auto delete_replicated_state_trx(nil::dbms::agency::envelope envelope, DatabaseID const &database, LogId id)
        -> nil::dbms::agency::envelope;
    auto deleteReplicatedState(DatabaseID const &database, LogId) -> futures::Future<ResultT<uint64_t>>;
    auto get_current_supervision(TRI_vocbase_t &vocbase, LogId id) -> log_current_supervision;

    auto replace_replicated_state_participant(std::string const &databaseName, LogId id,
                                              ParticipantId const &participantToRemove,
                                              ParticipantId const &participantToAdd,
                                              std::optional<ParticipantId> const &currentLeader)
        -> futures::Future<Result>;

    auto replace_replicated_set_leader(std::string const &databaseName, LogId id,
                                       std::optional<ParticipantId> const &leaderId) -> futures::Future<Result>;

}    // namespace nil::dbms::replication::agency::methods
