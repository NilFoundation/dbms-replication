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

#include "fake_failure_oracle.hpp"

using namespace nil::dbms::replication::test;

auto fake_failure_oracle::isServerFailed(std::string_view serverId) const noexcept -> bool {
    if (auto status = isFailed.find(std::string(serverId)); status != std::end(isFailed)) {
        return status->second;
    }
    // If a participant is not found, assume it's healthy. This serves as a
    // shortcut, so we only have to care about failed participants while testing.
    return false;
}
