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
#include <chrono>
#include <optional>
#include <string>

#include <nil/dbms/replication/log/log_status.hpp>
#include <nil/dbms/replication/state/state_common.hpp>

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication::state {

    namespace static_strings {
        inline constexpr auto StringDetail = std::string_view {"detail"};
        inline constexpr auto StringManager = std::string_view {"manager"};
        inline constexpr auto StringLastChange = std::string_view {"lastChange"};
        inline constexpr auto StringManagerState = std::string_view {"managerState"};
        inline constexpr auto StringSnapshot = std::string_view {"snapshot"};
        inline constexpr auto StringGeneration = std::string_view {"generation"};
        inline constexpr auto StringRole = std::string_view {"role"};
        inline constexpr auto StringUnconfigured = std::string_view {"unconfigured"};
        inline constexpr auto StringLeader = std::string_view {"leader"};
        inline constexpr auto StringFollower = std::string_view {"follower"};
    }    // namespace static_strings

    enum class leader_internal_state {
        kUninitializedState,
        kWaitingForLeadershipEstablished,
        kIngestingExistingLog,
        kRecoveryInProgress,
        kServiceAvailable,
    };

    auto to_string(leader_internal_state) noexcept -> std::string_view;
    struct leader_internal_stateStringTransformer {
        using SerializedType = std::string;
        auto toSerialized(leader_internal_state source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, leader_internal_state &target) const -> inspection::Status;
    };

    struct LeaderStatus {
        using clock = std::chrono::system_clock;

        struct ManagerState {
            leader_internal_state state {};
            clock::time_point lastChange {};
            std::optional<std::string> detail;
        };

        ManagerState managerState;
        StateGeneration generation;
        SnapshotInfo snapshot;
    };

    template<class Inspector>
    auto inspect(Inspector &f, LeaderStatus &x) {
        auto role = std::string {static_strings::StringLeader};
        return f.object(x).fields(f.field(static_strings::StringGeneration, x.generation),
                                  f.field(static_strings::StringSnapshot, x.snapshot),
                                  f.field(static_strings::StringManager, x.managerState),
                                  f.field(static_strings::StringRole, role));
    }

    template<class Inspector>
    auto inspect(Inspector &f, LeaderStatus::ManagerState &x) {
        return f.object(x).fields(
            f.field(static_strings::StringManagerState, x.state).transformWith(leader_internal_stateStringTransformer {}),
            f.field(static_strings::StringLastChange, x.lastChange).transformWith(inspection::TimeStampTransformer {}),
            f.field(static_strings::StringDetail, x.detail));
    }

    enum class follower_internal_state {
        kUninitializedState,
        kWaitForLeaderConfirmation,
        kTransferSnapshot,
        kNothingToApply,
        kApplyRecentEntries,
        kSnapshotTransferFailed,
    };

    auto to_string(follower_internal_state) noexcept -> std::string_view;
    struct follower_internal_stateStringTransformer {
        using SerializedType = std::string;
        auto toSerialized(follower_internal_state source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, follower_internal_state &target) const -> inspection::Status;
    };

    struct FollowerStatus {
        using clock = std::chrono::system_clock;

        struct ManagerState {
            follower_internal_state state {};
            clock::time_point lastChange {};
            std::optional<std::string> detail;
        };

        ManagerState managerState;
        StateGeneration generation;
        SnapshotInfo snapshot;
    };

    template<class Inspector>
    auto inspect(Inspector &f, FollowerStatus &x) {
        auto role = std::string {static_strings::StringFollower};
        return f.object(x).fields(f.field(static_strings::StringGeneration, x.generation),
                                  f.field(static_strings::StringSnapshot, x.snapshot),
                                  f.field(static_strings::StringManager, x.managerState),
                                  f.field(static_strings::StringRole, role));
    }

    template<class Inspector>
    auto inspect(Inspector &f, FollowerStatus::ManagerState &x) {
        return f.object(x).fields(
            f.field(static_strings::StringManagerState, x.state)
                .transformWith(follower_internal_stateStringTransformer {}),
            f.field(static_strings::StringLastChange, x.lastChange).transformWith(inspection::TimeStampTransformer {}),
            f.field(static_strings::StringDetail, x.detail));
    }

    struct UnconfiguredStatus {
        StateGeneration generation;
        SnapshotInfo snapshot;

        bool operator==(const UnconfiguredStatus &rhs) const {
            return generation == rhs.generation && snapshot == rhs.snapshot;
        }
        bool operator!=(const UnconfiguredStatus &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, UnconfiguredStatus &x) {
        auto role = std::string {static_strings::StringUnconfigured};
        return f.object(x).fields(f.field(static_strings::StringGeneration, x.generation),
                                  f.field(static_strings::StringSnapshot, x.snapshot),
                                  f.field(static_strings::StringRole, role));
    }

    struct StateStatus {
        std::variant<LeaderStatus, FollowerStatus, UnconfiguredStatus> variant;

        [[nodiscard]] auto asFollowerStatus() const noexcept -> FollowerStatus const * {
            return std::get_if<FollowerStatus>(&variant);
        }

        [[nodiscard]] auto get_snapshotInfo() const noexcept -> SnapshotInfo const & {
            return std::visit([](auto &&s) -> SnapshotInfo const & { return s.snapshot; }, variant);
        }

        [[nodiscard]] auto getGeneration() const noexcept -> StateGeneration {
            return std::visit([](auto &&s) -> StateGeneration { return s.generation; }, variant);
        }

        void to_velocy_pack(velocypack::Builder &) const;
        static auto from_velocy_pack(velocypack::Slice) -> StateStatus;

        friend auto operator<<(std::ostream &, StateStatus const &) -> std::ostream &;
    };

    auto operator<<(std::ostream &, StateStatus const &) -> std::ostream &;

}    // namespace nil::dbms::replication::state
