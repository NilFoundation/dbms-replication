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

#include <nil/replication_sdk/replicated_log/ilog_interfaces.hpp>
#include <nil/replication_sdk/replicated_log/log_core.hpp>
#include <nil/replication_sdk/replicated_log/log_status.hpp>
#include <nil/replication_sdk/replicated_log/replicated_log_metrics.hpp>

#include <basics/static_strings.h>

using namespace nil::dbms;
using namespace nil::dbms::replication_sdk;

auto replicated_log::ilog_participant::getTerm() const noexcept -> std::optional<log_term> {
    return getQuickStatus().getCurrentTerm();
}

replicated_log::wait_for_result::wait_for_result(log_index index, std::shared_ptr<quorum_data const> quorum) :
    currentCommitIndex(index), quorum(std::move(quorum)) {
}

void replicated_log::wait_for_result::toVelocyPack(velocypack::Builder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add(StaticStrings::CommitIndex, VPackValue(currentCommitIndex));
    builder.add(VPackValue("quorum"));
    quorum->toVelocyPack(builder);
}

replicated_log::wait_for_result::wait_for_result(velocypack::Slice s) {
    currentCommitIndex = s.get(StaticStrings::CommitIndex).extract<log_index>();
    quorum = std::make_shared<quorum_data>(s.get("quorum"));
}
