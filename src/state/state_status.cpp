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

#include <nil/dbms/replication/state/state_status.hpp>

#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "basics/voc_errors.h"
#include "basics/time_string.h"

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;

namespace {
    inline constexpr std::string_view StringWaitingForLeadershipEstablished = "WaitingForLeadershipEstablished";
    inline constexpr std::string_view StringIngestingExistingLog = "IngestingExistingLog";
    inline constexpr std::string_view StringRecoveryInProgress = "RecoveryInProgress";
    inline constexpr std::string_view StringServiceAvailable = "ServiceAvailable";

    inline constexpr std::string_view StringWaitForLeaderConfirmation = "WaitForLeaderConfirmation";
    inline constexpr std::string_view StringTransferSnapshot = "TransferSnapshot";
    inline constexpr std::string_view StringNothingToApply = "NothingToApply";
    inline constexpr std::string_view StringApplyRecentEntries = "ApplyRecentEntries";
    inline constexpr std::string_view StringUninitializedState = "UninitializedState";
    inline constexpr std::string_view StringSnapshotTransferFailed = "SnapshotTransferFailed";

}    // namespace

auto state::to_string(LeaderInternalState state) noexcept -> std::string_view {
    switch (state) {
        case LeaderInternalState::kWaitingForLeadershipEstablished:
            return StringWaitingForLeadershipEstablished;
        case LeaderInternalState::kIngestingExistingLog:
            return StringIngestingExistingLog;
        case LeaderInternalState::kRecoveryInProgress:
            return StringRecoveryInProgress;
        case LeaderInternalState::kServiceAvailable:
            return StringServiceAvailable;
        case LeaderInternalState::kUninitializedState:
            return StringUninitializedState;
    }
    TRI_ASSERT(false) << "invalid state value " << int(state);
    return "(unknown-internal-leader-state)";
}

auto state::to_string(FollowerInternalState state) noexcept -> std::string_view {
    switch (state) {
        case FollowerInternalState::kWaitForLeaderConfirmation:
            return StringWaitForLeaderConfirmation;
        case FollowerInternalState::kTransferSnapshot:
            return StringTransferSnapshot;
        case FollowerInternalState::kNothingToApply:
            return StringNothingToApply;
        case FollowerInternalState::kApplyRecentEntries:
            return StringApplyRecentEntries;
        case FollowerInternalState::kUninitializedState:
            return StringUninitializedState;
        case FollowerInternalState::kSnapshotTransferFailed:
            return StringSnapshotTransferFailed;
    }
    TRI_ASSERT(false) << "invalid state value " << int(state);
    return "(unknown-internal-follower-state)";
}

void StateStatus::to_velocy_pack(velocypack::Builder &builder) const {
    std::visit([&](auto const &s) { velocypack::serialize(builder, s); }, variant);
}

auto StateStatus::from_velocy_pack(velocypack::Slice slice) -> StateStatus {
    auto role = slice.get(static_strings::StringRole).stringView();
    if (role == StaticStrings::Leader) {
        return StateStatus {velocypack::deserialize<LeaderStatus>(slice)};
    } else if (role == StaticStrings::Follower) {
        return StateStatus {velocypack::deserialize<FollowerStatus>(slice)};
    } else if (role == static_strings::StringUnconfigured) {
        return StateStatus {velocypack::deserialize<UnconfiguredStatus>(slice)};
    } else {
        THROW_DBMS_EXCEPTION_FORMAT(TRI_ERROR_BAD_PARAMETER, "unknown role %*s", role.size(), role.data());
    }
}

auto nil::dbms::replication::state::operator<<(std::ostream &out, StateStatus const &stateStatus)
    -> std::ostream & {
    VPackBuilder builder;
    stateStatus.to_velocy_pack(builder);
    return out << builder.slice().toJson();
}

auto FollowerInternalStateStringTransformer::toSerialized(FollowerInternalState source, std::string &target) const
    -> inspection::Status {
    target = to_string(source);
    return {};
}

auto FollowerInternalStateStringTransformer::fromSerialized(std::string const &source,
                                                            FollowerInternalState &target) const -> inspection::Status {
    if (source == StringUninitializedState) {
        target = FollowerInternalState::kUninitializedState;
    } else if (source == StringWaitForLeaderConfirmation) {
        target = FollowerInternalState::kWaitForLeaderConfirmation;
    } else if (source == StringTransferSnapshot) {
        target = FollowerInternalState::kTransferSnapshot;
    } else if (source == StringNothingToApply) {
        target = FollowerInternalState::kNothingToApply;
    } else if (source == StringApplyRecentEntries) {
        target = FollowerInternalState::kApplyRecentEntries;
    } else if (source == StringSnapshotTransferFailed) {
        target = FollowerInternalState::kSnapshotTransferFailed;
    } else {
        return inspection::Status {"unknown follower internal state " + source};
    }
    return {};
}

auto LeaderInternalStateStringTransformer::toSerialized(LeaderInternalState source, std::string &target) const
    -> inspection::Status {
    target = to_string(source);
    return {};
}

auto LeaderInternalStateStringTransformer::fromSerialized(std::string const &source, LeaderInternalState &target) const
    -> inspection::Status {
    if (source == StringUninitializedState) {
        target = LeaderInternalState::kUninitializedState;
    } else if (source == StringIngestingExistingLog) {
        target = LeaderInternalState::kIngestingExistingLog;
    } else if (source == StringRecoveryInProgress) {
        target = LeaderInternalState::kRecoveryInProgress;
    } else if (source == StringServiceAvailable) {
        target = LeaderInternalState::kServiceAvailable;
    } else if (source == StringWaitingForLeadershipEstablished) {
        target = LeaderInternalState::kWaitingForLeadershipEstablished;
    } else {
        return inspection::Status {"unknown leader internal state " + source};
    }
    return {};
}
