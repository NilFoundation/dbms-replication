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
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/log/supervision_action.hpp>

#include <fmt/core.h>
#include <logger/LogMacros.h>

namespace nil::dbms::replication::log {

    struct SupervisionContext {
        SupervisionContext() = default;
        SupervisionContext(SupervisionContext const &) = delete;
        SupervisionContext(SupervisionContext &&) noexcept = delete;
        SupervisionContext &operator=(SupervisionContext const &) = delete;
        SupervisionContext &operator=(SupervisionContext &&) noexcept = delete;

        template<typename ActionType, typename... Args>
        void createAction(Args &&...args) {
            if (std::holds_alternative<EmptyAction>(_action)) {
                _action.emplace<ActionType>(ActionType {std::forward<Args>(args)...});
            }
        }

        template<typename StatusType, typename... Args>
        void reportStatus(Args &&...args) {
            if (_isErrorReportingEnabled) {
                _reports.emplace_back(StatusType {std::forward<Args>(args)...});
            }
        }

        void enableErrorReporting() noexcept {
            _isErrorReportingEnabled = true;
        }

        auto hasAction() noexcept -> bool {
            return not std::holds_alternative<EmptyAction>(_action);
        }

        auto hasModifyingAction() const noexcept -> bool {
            return not(std::holds_alternative<EmptyAction>(_action) or
                       std::holds_alternative<NoActionPossibleAction>(_action));
        }

        auto getAction() -> Action & {
            return _action;
        }
        auto getReport() noexcept -> LogCurrentSupervision::StatusReport & {
            return _reports;
        }

        auto hasUpdates() noexcept -> bool {
            return hasModifyingAction() or !_reports.empty();
        }

        auto isErrorReportingEnabled() const noexcept {
            return _isErrorReportingEnabled;
        }

        std::size_t numberServersInTarget;
        std::size_t numberServersOk;

    private:
        bool _isErrorReportingEnabled {true};
        Action _action;
        LogCurrentSupervision::StatusReport _reports;
    };

}    // namespace nil::dbms::replication::log
