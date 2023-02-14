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

#include <nil/replication_sdk/replicated_state/agency_specification.hpp>
#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "basics/debugging.h"
#include "basics/voc_errors.h"
#include "velocypack/Builder.h"
#include "velocypack/Iterator.h"

#include "logger/LogMacros.h"
#include "inspection/vpack.h"

using namespace nil::dbms;
using namespace nil::dbms::replication_sdk;
using namespace nil::dbms::replication_sdk::replicated_state;
using namespace nil::dbms::replication_sdk::replicated_state::agency;

auto StatusCodeStringTransformer::toSerialized(StatusCode source, std::string &target) const -> inspection::Status {
    target = to_string(source);
    return {};
}

auto StatusCodeStringTransformer::fromSerialized(std::string const &source, StatusCode &target) const
    -> inspection::Status {
    if (source == "LogNotCreated") {
        target = StatusCode::kLogNotCreated;
    } else if (source == "LogCurrentNotAvailable") {
        target = StatusCode::kLogCurrentNotAvailable;
    } else if (source == "ServerSnapshotMissing") {
        target = StatusCode::kServerSnapshotMissing;
    } else if (source == "InsufficientSnapshotCoverage") {
        target = StatusCode::kInsufficientSnapshotCoverage;
    } else if (source == "LogParticipantNotYetGone") {
        target = StatusCode::kLogParticipantNotYetGone;
    } else {
        return inspection::Status {"Invalid status code name " + std::string {source}};
    }
    return {};
}

auto replicated_state::agency::to_string(StatusCode code) noexcept -> std::string_view {
    switch (code) {
        case Current::Supervision::kLogNotCreated:
            return "LogNotCreated";
        case Current::Supervision::kLogCurrentNotAvailable:
            return "LogCurrentNotAvailable";
        case Current::Supervision::kServerSnapshotMissing:
            return "ServerSnapshotMissing";
        case Current::Supervision::kInsufficientSnapshotCoverage:
            return "InsufficientSnapshotCoverage";
        case Current::Supervision::kLogParticipantNotYetGone:
            return "LogParticipantNotYetGone";
        default:
            return "(unknown status code)";
    }
}
