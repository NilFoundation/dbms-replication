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

#include "types.hpp"

namespace nil::dbms::network {
    class ConnectionPool;
}

namespace nil::dbms::replication::replicated_log {

    struct network_attached_follower : nil::dbms::replication::replicated_log::abstract_follower {
        explicit network_attached_follower(network::ConnectionPool *pool, ParticipantId id, DatabaseID database,
                                           LogId logId);
        [[nodiscard]] auto getParticipantId() const noexcept -> ParticipantId const & override;
        auto appendEntries(append_entries_request request) -> futures::Future<append_entries_result> override;

    private:
        network::ConnectionPool *pool;
        ParticipantId id;
        DatabaseID database;
        LogId logId;
    };

}    // namespace nil::dbms::replication::replicated_log
