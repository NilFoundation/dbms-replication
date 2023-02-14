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

#include <variant>
#include <nil/replication_sdk/replicated_log/agency_log_specification.hpp>
#include <nil/replication_sdk/replicated_log/supervision_action.hpp>

#include <fmt/core.h>
#include <logger/LogMacros.h>

namespace nil::dbms::replication_sdk::replicated_log {

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

        template<typename StatusType, typename... Args>
        void report_status(Args &&...args) {
            if (_isErrorReportingEnabled) {
                _reports.emplace_back(StatusType {std::forward<Args>(args)...});
            }
        }

        void enable_error_reporting() noexcept {
            _isErrorReportingEnabled = true;
        }

        auto has_action() noexcept -> bool {
            return not std::holds_alternative<empty_action>(_action);
        }

        auto has_modifying_action() const noexcept -> bool {
            return not(std::holds_alternative<empty_action>(_action) or
                       std::holds_alternative<no_action_possible_action>(_action));
        }

        auto get_action() -> Action & {
            return _action;
        }
        auto get_report() noexcept -> log_current_supervision::StatusReport & {
            return _reports;
        }

        auto has_updates() noexcept -> bool {
            return has_modifying_action() or !_reports.empty();
        }

        auto is_error_reporting_enabled() const noexcept {
            return _isErrorReportingEnabled;
        }

        std::size_t numberServersInTarget;
        std::size_t numberServersOk;

    private:
        bool _isErrorReportingEnabled {true};
        Action _action;
        log_current_supervision::StatusReport _reports;
    };

}    // namespace nil::dbms::replication_sdk::replicated_log
