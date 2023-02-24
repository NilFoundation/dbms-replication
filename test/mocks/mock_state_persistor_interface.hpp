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
#include <mutex>
#include <optional>
#include <nil/dbms/replication/replicated_state/persisted_state_info.hpp>

namespace nil::dbms::replication::test {

    struct mock_state_persistor_interface : replicated_state::state_persistor_interface {
        void update_state_information(const replicated_state::persisted_state_info &info) noexcept override;
        void delete_state_information(LogId stateId) noexcept override;

        std::mutex _mutex;
        std::optional<replicated_state::persisted_state_info> _info;
    };

}    // namespace nil::dbms::replication::test
