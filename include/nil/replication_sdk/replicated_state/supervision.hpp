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
#include <optional>

#include <nil/replication_sdk/replicated_log/agency_log_specification.hpp>
#include <nil/replication_sdk/replicated_state/agency_specification.hpp>
#include <nil/replication_sdk/replicated_state/supervision_action.hpp>

namespace nil::dbms::agency {
    struct envelope;

}

namespace nil::dbms::replication_sdk::replicated_state {

    struct supervision_context {
        supervision_context() = default;
        supervision_context(supervision_context const &) = delete;
        supervision_context(supervision_context &&) noexcept = delete;
        supervision_context &operator=(supervision_context const &) = delete;
        supervision_context &operator=(supervision_context &&) noexcept = delete;

        template<typename ActionType, typename... Args>
        void create_action(Args &&...args) {
            if (std::holds_alternative<empty_action>(_action)) {
                _action.emplace<ActionType>(ActionType {std::forward<Args>(args)...});
            }
        }

        void report_status(agency::StatusCode code, std::optional<ParticipantId> participant) {
            if (_isErrorReportingEnabled) {
                _reports.emplace_back(code, std::move(participant));
            }
        }

        void enable_error_reporting() noexcept {
            _isErrorReportingEnabled = true;
        }

        auto get_action() noexcept -> Action & {
            return _action;
        }
        auto get_report() noexcept -> agency::StatusReport & {
            return _reports;
        }

        auto has_updates() noexcept -> bool {
            return !std::holds_alternative<empty_action>(_action) || !_reports.empty();
        }

        auto is_error_reporting_enabled() const noexcept {
            return _isErrorReportingEnabled;
        }

        std::size_t numberServersInTarget;
        std::size_t numberServersOk;

    private:
        bool _isErrorReportingEnabled {false};
        Action _action;
        agency::StatusReport _reports;
    };

    void check_replicated_state(supervision_context &ctx,
                              std::optional<nil::dbms::replication_sdk::agency::Log> const &log,
                              agency::State const &state);

    auto executeCheckReplicatedState(DatabaseID const &database, agency::State state,
                                     std::optional<nil::dbms::replication_sdk::agency::Log> log,
                                     nil::dbms::agency::envelope) noexcept -> nil::dbms::agency::envelope;

}    // namespace nil::dbms::replication_sdk::replicated_state
