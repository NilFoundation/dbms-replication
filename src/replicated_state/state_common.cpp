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

#include <nil/replication_sdk/replicated_state/state_common.hpp>

#include <velocypack/Value.h>
#include <velocypack/Builder.h>

#include "basics/debugging.h"
#include "basics/static_strings.h"
#include "basics/time_string.h"

using namespace nil::dbms::replication_sdk;
using namespace nil::dbms::replication_sdk::replicated_state;

namespace {
    auto const String_InProgress = std::string_view {"InProgress"};
    auto const String_Completed = std::string_view {"Completed"};
    auto const String_Failed = std::string_view {"Failed"};
    auto const String_Uninitialized = std::string_view {"Uninitialized"};
}    // namespace

state_generation::operator nil::dbms::velocypack::Value() const noexcept {
    return nil::dbms::velocypack::Value(value);
}

auto state_generation::operator+(std::uint64_t delta) const -> state_generation {
    return state_generation {value + delta};
}

auto state_generation::saturated_decrement(uint64_t delta) const noexcept -> state_generation {
    if (value > delta) {
        return state_generation {value - delta};
    }
    return state_generation {0};
}

auto replicated_state::operator<<(std::ostream &os, state_generation g) -> std::ostream & {
    return os << g.value;
}

auto state_generation::operator++() noexcept -> state_generation & {
    ++value;
    return *this;
}

auto state_generation::operator++(int) noexcept -> state_generation {
    return state_generation {value++};
}

auto replicated_state::operator<<(std::ostream &os, snapshot_status const &ss) -> std::ostream & {
    return os << to_string(ss);
}

void snapshot_info::updateStatus(snapshot_status s) noexcept {
    if (status != s) {
        status = s;
        timestamp = clock::now();
    }
}

auto replicated_state::to_string(snapshot_status s) noexcept -> std::string_view {
    switch (s) {
        case snapshot_status::kUninitialized:
            return String_Uninitialized;
        case snapshot_status::kInProgress:
            return String_InProgress;
        case snapshot_status::kCompleted:
            return String_Completed;
        case snapshot_status::kFailed:
            return String_Failed;
        default:
            return "(unknown snapshot status)";
    }
}

auto replicated_state::snapshotStatusFromString(std::string_view string) noexcept -> snapshot_status {
    if (string == String_InProgress) {
        return snapshot_status::kInProgress;
    } else if (string == String_Completed) {
        return snapshot_status::kCompleted;
    } else if (string == String_Failed) {
        return snapshot_status::kFailed;
    } else {
        return snapshot_status::kUninitialized;
    }
}

auto replicated_state::to_string(state_generation g) -> std::string {
    return std::to_string(g.value);
}

auto SnapshotStatusStringTransformer::toSerialized(snapshot_status source, std::string &target) const
    -> inspection::Status {
    target = to_string(source);
    return {};
}

auto SnapshotStatusStringTransformer::fromSerialized(std::string const &source, snapshot_status &target) const
    -> inspection::Status {
    if (source == String_InProgress) {
        target = snapshot_status::kInProgress;
    } else if (source == String_Completed) {
        target = snapshot_status::kCompleted;
    } else if (source == String_Failed) {
        target = snapshot_status::kFailed;
    } else if (source == String_Uninitialized) {
        target = snapshot_status::kUninitialized;
    } else {
        return inspection::Status {"Invalid status code name " + std::string {source}};
    }
    return {};
}
