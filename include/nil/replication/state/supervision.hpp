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

#include <nil/dbms/agency/transaction_builder.hpp>
#include <nil/dbms/replication/log/agency_log_specification.hpp>
#include <nil/dbms/replication/state/agency_specification.hpp>
#include <nil/dbms/replication/state/supervision_action.hpp>

namespace nil::dbms::replication::state {

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

        void reportStatus(agency::StatusCode code, std::optional<ParticipantId> participant) {
            if (_isErrorReportingEnabled) {
                _reports.emplace_back(code, std::move(participant));
            }
        }

        void enableErrorReporting() noexcept {
            _isErrorReportingEnabled = true;
        }

        auto getAction() noexcept -> Action & {
            return _action;
        }
        auto getReport() noexcept -> agency::StatusReport & {
            return _reports;
        }

        auto hasUpdates() noexcept -> bool {
            return !std::holds_alternative<EmptyAction>(_action) || !_reports.empty();
        }

        auto isErrorReportingEnabled() const noexcept {
            return _isErrorReportingEnabled;
        }

        std::size_t numberServersInTarget;
        std::size_t numberServersOk;

    private:
        bool _isErrorReportingEnabled {false};
        Action _action;
        agency::StatusReport _reports;
    };

    void checkReplicatedState(SupervisionContext &ctx,
                              std::optional<nil::dbms::replication::agency::Log> const &log,
                              agency::State const &state);

    auto executeCheckReplicatedState(DatabaseID const &database, agency::State state,
                                     std::optional<nil::dbms::replication::agency::Log> log,
                                     nil::dbms::agency::envelope) noexcept -> nil::dbms::agency::envelope;

}    // namespace nil::dbms::replication::state
