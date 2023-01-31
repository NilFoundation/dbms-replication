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

#include <nil/dbms/replication/state/supervision_action.hpp>
#include <nil/dbms/agency/agency_paths.hpp>
#include <nil/dbms/agency/transaction_builder.hpp>
#include "basics/string_utils.h"
#include "velocypack/Builder.h"

namespace RLA = nil::dbms::replication::agency;
namespace RSA = nil::dbms::replication::state::agency;
namespace paths = nil::dbms::cluster::paths::aliases;

using namespace nil::dbms::replication;

namespace nil::dbms::replication::state {

    auto executeAction(RSA::State state, std::optional<RLA::Log> log, Action &action) -> ActionContext {
        auto logTarget = std::invoke([&]() -> std::optional<RLA::LogTarget> {
            if (log) {
                return std::move(log->target);
            }
            return std::nullopt;
        });

        auto statePlan = std::invoke([&]() -> std::optional<RSA::Plan> {
            if (state.plan) {
                return std::move(state.plan);
            }
            return std::nullopt;
        });

        auto currentSupervision = std::invoke([&]() -> std::optional<RSA::Current::Supervision> {
            if (state.current) {
                return std::move(state.current->supervision);
            }
            return RSA::Current::Supervision {};
        });

        auto actionCtx =
            state::ActionContext {std::move(logTarget), std::move(statePlan), std::move(currentSupervision)};
        std::visit([&](auto &action) { action.execute(actionCtx); }, action);
        return actionCtx;
    }
}    // namespace nil::dbms::replication::state
