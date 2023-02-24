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

#include "fake_replicated_log.hpp"
#include "fake_failure_oracle.hpp"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_log;
using namespace nil::dbms::replication::test;

auto test_replicated_log::become_follower(ParticipantId const &id, log_term term, ParticipantId leaderId)
    -> std::shared_ptr<DelayedFollowerLog> {
    auto ptr = replicated_log_t::become_follower(id, term, std::move(leaderId));
    return std::make_shared<DelayedFollowerLog>(ptr);
}

auto test_replicated_log::become_leader(ParticipantId const &id, log_term term,
                                      std::vector<std::shared_ptr<replicated_log::abstract_follower>> const &follower,
                                      std::size_t effectiveWriteConcern, bool waitForSync,
                                      std::shared_ptr<cluster::IFailureOracle> failureOracle)
    -> std::shared_ptr<replicated_log::log_leader> {
    agency::log_plan_config config;
    config.effectiveWriteConcern = effectiveWriteConcern;
    config.waitForSync = waitForSync;

    auto participants = std::unordered_map<ParticipantId, participant_flags> {{id, {}}};
    for (auto const &participant : follower) {
        participants.emplace(participant->getParticipantId(), participant_flags {});
    }
    auto participantsConfig = std::make_shared<agency::participants_config>(
        agency::participants_config {.generation = 1, .participants = std::move(participants), .config = config});

    if (!failureOracle) {
        failureOracle = std::make_shared<fake_failure_oracle>();
    }

    return become_leader(config, id, term, follower, std::move(participantsConfig), failureOracle);
}
