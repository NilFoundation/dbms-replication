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

#include <nil/dbms/replication/state/state_common.hpp>

#include <velocypack/Value.h>
#include <velocypack/Builder.h>

#include "basics/debugging.h"
#include "basics/static_strings.h"
#include "basics/time_string.h"

using namespace nil::dbms::replication;
using namespace nil::dbms::replication::state;

namespace {
    auto const String_InProgress = std::string_view {"InProgress"};
    auto const String_Completed = std::string_view {"Completed"};
    auto const String_Failed = std::string_view {"Failed"};
    auto const String_Uninitialized = std::string_view {"Uninitialized"};
}    // namespace

StateGeneration::operator nil::dbms::velocypack::Value() const noexcept {
    return nil::dbms::velocypack::Value(value);
}

auto StateGeneration::operator+(std::uint64_t delta) const -> StateGeneration {
    return StateGeneration {value + delta};
}

auto StateGeneration::saturatedDecrement(uint64_t delta) const noexcept -> StateGeneration {
    if (value > delta) {
        return StateGeneration {value - delta};
    }
    return StateGeneration {0};
}

auto state::operator<<(std::ostream &os, StateGeneration g) -> std::ostream & {
    return os << g.value;
}

auto StateGeneration::operator++() noexcept -> StateGeneration & {
    ++value;
    return *this;
}

auto StateGeneration::operator++(int) noexcept -> StateGeneration {
    return StateGeneration {value++};
}

auto state::operator<<(std::ostream &os, SnapshotStatus const &ss) -> std::ostream & {
    return os << to_string(ss);
}

void SnapshotInfo::updateStatus(SnapshotStatus s) noexcept {
    if (status != s) {
        status = s;
        timestamp = clock::now();
    }
}

auto state::to_string(SnapshotStatus s) noexcept -> std::string_view {
    switch (s) {
        case SnapshotStatus::kUninitialized:
            return String_Uninitialized;
        case SnapshotStatus::kInProgress:
            return String_InProgress;
        case SnapshotStatus::kCompleted:
            return String_Completed;
        case SnapshotStatus::kFailed:
            return String_Failed;
        default:
            return "(unknown snapshot status)";
    }
}

auto state::snapshotStatusFromString(std::string_view string) noexcept -> SnapshotStatus {
    if (string == String_InProgress) {
        return SnapshotStatus::kInProgress;
    } else if (string == String_Completed) {
        return SnapshotStatus::kCompleted;
    } else if (string == String_Failed) {
        return SnapshotStatus::kFailed;
    } else {
        return SnapshotStatus::kUninitialized;
    }
}

auto state::to_string(StateGeneration g) -> std::string {
    return std::to_string(g.value);
}

auto SnapshotStatusStringTransformer::toSerialized(SnapshotStatus source, std::string &target) const
    -> inspection::Status {
    target = to_string(source);
    return {};
}

auto SnapshotStatusStringTransformer::fromSerialized(std::string const &source, SnapshotStatus &target) const
    -> inspection::Status {
    if (source == String_InProgress) {
        target = SnapshotStatus::kInProgress;
    } else if (source == String_Completed) {
        target = SnapshotStatus::kCompleted;
    } else if (source == String_Failed) {
        target = SnapshotStatus::kFailed;
    } else if (source == String_Uninitialized) {
        target = SnapshotStatus::kUninitialized;
    } else {
        return inspection::Status {"Invalid status code name " + std::string {source}};
    }
    return {};
}
