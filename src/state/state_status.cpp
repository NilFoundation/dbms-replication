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

auto state::to_string(leader_internal_state state) noexcept -> std::string_view {
    switch (state) {
        case leader_internal_state::kWaitingForLeadershipEstablished:
            return StringWaitingForLeadershipEstablished;
        case leader_internal_state::kIngestingExistingLog:
            return StringIngestingExistingLog;
        case leader_internal_state::kRecoveryInProgress:
            return StringRecoveryInProgress;
        case leader_internal_state::kServiceAvailable:
            return StringServiceAvailable;
        case leader_internal_state::kUninitializedState:
            return StringUninitializedState;
    }
    TRI_ASSERT(false) << "invalid state value " << int(state);
    return "(unknown-internal-leader-state)";
}

auto state::to_string(follower_internal_state state) noexcept -> std::string_view {
    switch (state) {
        case follower_internal_state::kWaitForLeaderConfirmation:
            return StringWaitForLeaderConfirmation;
        case follower_internal_state::kTransferSnapshot:
            return StringTransferSnapshot;
        case follower_internal_state::kNothingToApply:
            return StringNothingToApply;
        case follower_internal_state::kApplyRecentEntries:
            return StringApplyRecentEntries;
        case follower_internal_state::kUninitializedState:
            return StringUninitializedState;
        case follower_internal_state::kSnapshotTransferFailed:
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

auto follower_internal_stateStringTransformer::toSerialized(follower_internal_state source, std::string &target) const
    -> inspection::Status {
    target = to_string(source);
    return {};
}

auto follower_internal_stateStringTransformer::fromSerialized(std::string const &source,
                                                            follower_internal_state &target) const -> inspection::Status {
    if (source == StringUninitializedState) {
        target = follower_internal_state::kUninitializedState;
    } else if (source == StringWaitForLeaderConfirmation) {
        target = follower_internal_state::kWaitForLeaderConfirmation;
    } else if (source == StringTransferSnapshot) {
        target = follower_internal_state::kTransferSnapshot;
    } else if (source == StringNothingToApply) {
        target = follower_internal_state::kNothingToApply;
    } else if (source == StringApplyRecentEntries) {
        target = follower_internal_state::kApplyRecentEntries;
    } else if (source == StringSnapshotTransferFailed) {
        target = follower_internal_state::kSnapshotTransferFailed;
    } else {
        return inspection::Status {"unknown follower internal state " + source};
    }
    return {};
}

auto leader_internal_stateStringTransformer::toSerialized(leader_internal_state source, std::string &target) const
    -> inspection::Status {
    target = to_string(source);
    return {};
}

auto leader_internal_stateStringTransformer::fromSerialized(std::string const &source, leader_internal_state &target) const
    -> inspection::Status {
    if (source == StringUninitializedState) {
        target = leader_internal_state::kUninitializedState;
    } else if (source == StringIngestingExistingLog) {
        target = leader_internal_state::kIngestingExistingLog;
    } else if (source == StringRecoveryInProgress) {
        target = leader_internal_state::kRecoveryInProgress;
    } else if (source == StringServiceAvailable) {
        target = leader_internal_state::kServiceAvailable;
    } else if (source == StringWaitingForLeadershipEstablished) {
        target = leader_internal_state::kWaitingForLeadershipEstablished;
    } else {
        return inspection::Status {"unknown leader internal state " + source};
    }
    return {};
}
