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

#include "prototype_state_machine.hpp"

namespace nil::dbms::replication::state::prototype {
    struct prototype_follower_state : IReplicatedFollowerState<prototype_state>,
                                    std::enable_shared_from_this<prototype_follower_state> {
        explicit prototype_follower_state(std::unique_ptr<prototype_core>, std::shared_ptr<iprototype_network_interface>);

        [[nodiscard]] auto resign() &&noexcept -> std::unique_ptr<prototype_core> override;

        auto acquireSnapshot(ParticipantId const &destination, log_index) noexcept -> futures::Future<Result> override;

        auto apply_entries(std::unique_ptr<EntryIterator> ptr) noexcept -> futures::Future<Result> override;

        auto get(std::string key, log_index waitForIndex) -> futures::Future<ResultT<std::optional<std::string>>>;
        auto get(std::vector<std::string> keys, log_index waitForIndex)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        logger_context const loggerContext;

    private:
        global_log_identifier const _log_identifier;
        std::shared_ptr<iprototype_network_interface> const _networkInterface;
        Guarded<std::unique_ptr<prototype_core>, basics::UnshackledMutex> _guarded_data;
    };
}    // namespace nil::dbms::replication::state::prototype
