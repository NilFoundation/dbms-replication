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

#include <nil/dbms/cluster/failure_oracle.hpp>

#include <string>
#include <unordered_map>

namespace nil::dbms::replication::test {
    struct fake_failure_oracle : nil::dbms::cluster::IFailureOracle {
        auto isServerFailed(std::string_view serverId) const noexcept -> bool override;

        std::unordered_map<std::string, bool> isFailed;
    };
}    // namespace nil::dbms::replication::test
