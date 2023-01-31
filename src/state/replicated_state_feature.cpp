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

#include <nil/dbms/replication/state/state_feature.hpp>

#include <features/ApplicationServer.h>
#include "basics/exceptions.h"
#include "basics/application_exit.h"
#include "basics/debugging.h"
#include "logger/LogContextKeys.h"
#include "logger/LogMacros.h"
#include "nil/dbms/replication/log/log.hpp"

using namespace nil::dbms;
using namespace nil::dbms::replication;

auto state::replicated_state_feature::create_replicated_state(std::string_view name,
                                                                     std::shared_ptr<log::ReplicatedLog>
                                                                         log,
                                                                     logger_context const &loggerContext)
    -> std::shared_ptr<replicated_state_base> {
    auto name_str = std::string {name};
    if (auto iter = factories.find(name_str); iter != std::end(factories)) {
        auto log_id = log->getId();
        auto lc = loggerContext.with<logContextKeyStateImpl>(name_str).with<logContextKeylog_id>(log_id);
        LOG_CTX("24af7", TRACE, lc) << "Creating replicated state of type `" << name << "`.";
        return iter->second->create_replicated_state(std::move(log), std::move(lc));
    }
    THROW_DBMS_EXCEPTION(TRI_ERROR_DBMS_DATA_SOURCE_NOT_FOUND);    // TODO fix error code
}

auto state::replicated_state_feature::create_replicated_state(std::string_view name,
                                                                     std::shared_ptr<log::ReplicatedLog>
                                                                         log) -> std::shared_ptr<replicated_state_base> {
    return create_replicated_state(name, std::move(log), logger_context(Logger::REPLICATED_STATE));
}

void state::replicated_state_feature::assert_was_inserted(std::string_view name, bool wasInserted) {
    if (!wasInserted) {
        LOG_TOPIC("5b761", FATAL, Logger::REPLICATED_STATE) << "register state type with duplicated name " << name;
        FATAL_ERROR_EXIT();
    }
}

state::replicated_state_app_feature::replicated_state_app_feature(Server &server) : DbmsdFeature {server, *this} {
}
