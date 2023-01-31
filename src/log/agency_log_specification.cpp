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

#include <nil/dbms/replication/log/agency_specification_inspectors.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>

#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "basics/application_exit.h"
#include "inspection/vpack.h"
#include "logger/LogMacros.h"
#include "logger/Logger.h"

#include <velocypack/Iterator.h>

#include <type_traits>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::agency;

log_plan_config::log_plan_config(std::size_t effectiveWriteConcern, bool waitForSync) noexcept :
    effectiveWriteConcern(effectiveWriteConcern), waitForSync(waitForSync) {
}

log_plan_config::log_plan_config(std::size_t writeConcern, std::size_t softWriteConcern, bool waitForSync) noexcept :
    effectiveWriteConcern(writeConcern), waitForSync(waitForSync) {
}

log_plan_term_specification::log_plan_term_specification(log_term term, std::optional<Leader> leader) :
    term(term), leader(std::move(leader)) {
}

log_plan_specification::log_plan_specification(log_id id, std::optional<log_plan_term_specification> term) :
    id(id), currentTerm(std::move(term)) {
}

log_plan_specification::log_plan_specification(log_id id, std::optional<log_plan_term_specification> term,
                                           participants_config participants_config) :
    id(id),
    currentTerm(std::move(term)), participants_config(std::move(participants_config)) {
}

log_current_local_state::log_current_local_state(log_term term, term_index_pair spearhead) noexcept :
    term(term), spearhead(spearhead) {
}

auto agency::to_string(log_current_supervision_election::ErrorCode ec) noexcept -> std::string_view {
    switch (ec) {
        case log_current_supervision_election::ErrorCode::OK:
            return "the server is ok";
        case log_current_supervision_election::ErrorCode::SERVER_NOT_GOOD:
            return "the server is not reported as good in Supervision/Health";
        case log_current_supervision_election::ErrorCode::TERM_NOT_CONFIRMED:
            return "the server has not (yet) confirmed the current term";
        case log_current_supervision_election::ErrorCode::SERVER_EXCLUDED:
            return "the server is configured as excluded";
    }
    LOG_TOPIC("7e572", FATAL, nil::dbms::Logger::replication)
        << "Invalid log_current_supervision_election::ErrorCode " << static_cast<std::underlying_type_t<decltype(ec)>>(ec);
    FATAL_ERROR_ABORT();
}

LogTargetConfig::LogTargetConfig(std::size_t writeConcern, std::size_t softWriteConcern, bool waitForSync) noexcept :
    writeConcern(writeConcern), softWriteConcern(softWriteConcern), waitForSync(waitForSync) {
}

LogTarget::LogTarget(log_id id, ParticipantsFlagsMap const &participants, LogTargetConfig const &config) :
    id {id}, participants {participants}, config(config) {
}
