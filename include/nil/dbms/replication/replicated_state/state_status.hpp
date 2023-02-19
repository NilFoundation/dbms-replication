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

#include <nil/dbms/replication/replicated_log/log_status.hpp>
#include "state_common.hpp"

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication::replicated_state {

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
    struct leader_internal_state_string_transformer {
        using SerializedType = std::string;
        auto toSerialized(leader_internal_state source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, leader_internal_state &target) const -> inspection::Status;
    };

    struct leader_status {
        using clock = std::chrono::system_clock;

        struct manager_state {
            leader_internal_state state {};
            clock::time_point lastChange {};
            std::optional<std::string> detail;
        };

        manager_state managerState;
        state_generation generation;
        snapshot_info snapshot;
    };

    template<class Inspector>
    auto inspect(Inspector &f, leader_status &x) {
        auto role = std::string {static_strings::StringLeader};
        return f.object(x).fields(f.field(static_strings::StringGeneration, x.generation),
                                  f.field(static_strings::StringSnapshot, x.snapshot),
                                  f.field(static_strings::StringManager, x.managerState),
                                  f.field(static_strings::StringRole, role));
    }

    template<class Inspector>
    auto inspect(Inspector &f, leader_status::manager_state &x) {
        return f.object(x).fields(
            f.field(static_strings::StringManagerState, x.state)
                .transformWith(leader_internal_state_string_transformer {}),
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
    struct follower_internal_state_string_transformer {
        using SerializedType = std::string;
        auto toSerialized(follower_internal_state source, std::string &target) const -> inspection::Status;
        auto fromSerialized(std::string const &source, follower_internal_state &target) const -> inspection::Status;
    };

    struct follower_status {
        using clock = std::chrono::system_clock;

        struct manager_state {
            follower_internal_state state {};
            clock::time_point lastChange {};
            std::optional<std::string> detail;
        };

        manager_state managerState;
        state_generation generation;
        snapshot_info snapshot;
    };

    template<class Inspector>
    auto inspect(Inspector &f, follower_status &x) {
        auto role = std::string {static_strings::StringFollower};
        return f.object(x).fields(f.field(static_strings::StringGeneration, x.generation),
                                  f.field(static_strings::StringSnapshot, x.snapshot),
                                  f.field(static_strings::StringManager, x.managerState),
                                  f.field(static_strings::StringRole, role));
    }

    template<class Inspector>
    auto inspect(Inspector &f, follower_status::manager_state &x) {
        return f.object(x).fields(
            f.field(static_strings::StringManagerState, x.state)
                .transformWith(follower_internal_state_string_transformer {}),
            f.field(static_strings::StringLastChange, x.lastChange).transformWith(inspection::TimeStampTransformer {}),
            f.field(static_strings::StringDetail, x.detail));
    }

    struct unconfigured_status {
        state_generation generation;
        snapshot_info snapshot;

        bool operator==(const unconfigured_status &rhs) const {
            return generation == rhs.generation && snapshot == rhs.snapshot;
        }
        bool operator!=(const unconfigured_status &rhs) const {
            return !(rhs == *this);
        }
    };

    template<class Inspector>
    auto inspect(Inspector &f, unconfigured_status &x) {
        auto role = std::string {static_strings::StringUnconfigured};
        return f.object(x).fields(f.field(static_strings::StringGeneration, x.generation),
                                  f.field(static_strings::StringSnapshot, x.snapshot),
                                  f.field(static_strings::StringRole, role));
    }

    struct state_status {
        std::variant<leader_status, follower_status, unconfigured_status> variant;

        [[nodiscard]] auto asFollowerStatus() const noexcept -> follower_status const * {
            return std::get_if<follower_status>(&variant);
        }

        [[nodiscard]] auto getSnapshotInfo() const noexcept -> snapshot_info const & {
            return std::visit([](auto &&s) -> snapshot_info const & { return s.snapshot; }, variant);
        }

        [[nodiscard]] auto getGeneration() const noexcept -> state_generation {
            return std::visit([](auto &&s) -> state_generation { return s.generation; }, variant);
        }

        void toVelocyPack(velocypack::Builder &) const;
        static auto fromVelocyPack(velocypack::Slice) -> state_status;

        friend auto operator<<(std::ostream &, state_status const &) -> std::ostream &;
    };

    auto operator<<(std::ostream &, state_status const &) -> std::ostream &;

}    // namespace nil::dbms::replication::replicated_state
