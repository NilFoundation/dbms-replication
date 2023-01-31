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

#include <nil/dbms/replication/log/types.hpp>
#include <nil/dbms/cluster/cluster_types.hpp>

namespace nil::dbms::network {
    class ConnectionPool;
}

namespace nil::dbms::replication::log {

    struct NetworkAttachedFollower : nil::dbms::replication::log::AbstractFollower {
        explicit NetworkAttachedFollower(network::ConnectionPool *pool, ParticipantId id, DatabaseID database,
                                         log_id log_id);
        [[nodiscard]] auto getParticipantId() const noexcept -> ParticipantId const & override;
        auto appendEntries(AppendEntriesRequest request) -> futures::Future<AppendEntriesResult> override;

    private:
        network::ConnectionPool *pool;
        ParticipantId id;
        DatabaseID database;
        log_id log_id;
    };

}    // namespace nil::dbms::replication::log
