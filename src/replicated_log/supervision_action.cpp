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

#include <velocypack/Builder.h>
#include <velocypack/velocypack-common.h>

#include "inspection/vpack.h"
#include "logger/LogMacros.h"

#include <nil/dbms/replication/replicated_log/agency_log_specification.hpp>
#include <nil/dbms/replication/replicated_log/agency_specification_inspectors.hpp>
#include <nil/dbms/replication/replicated_log/supervision_action.hpp>

#include <fmt/core.h>
#include <variant>

using namespace nil::dbms::replication::agency;
// namespace paths = nil::dbms::cluster::paths::aliases;

namespace nil::dbms::replication::replicated_log {

    auto executeAction(Log log, Action &action) -> ActionContext {
        auto currentSupervision = std::invoke([&]() -> std::optional<log_current_supervision> {
            if (log.current.has_value()) {
                return log.current->supervision;
            } else {
                return std::nullopt;
            }
        });

        if (!currentSupervision.has_value()) {
            currentSupervision.emplace();
        }

        auto ctx = ActionContext {std::move(log.plan), std::move(currentSupervision)};

        std::visit([&](auto &&action) { action.execute(ctx); }, action);
        return ctx;
    }

}    // namespace nil::dbms::replication::replicated_log
