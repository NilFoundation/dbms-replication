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

#include <utility>

#include <nil/dbms/network/connection_pool.hpp>
#include <nil/dbms/network/methods.hpp>

#include <nil/dbms/replication/log/network_messages.hpp>
#include <nil/dbms/replication/log/network_attached_follower.hpp>
#include "basics/string_utils.h"
#include "basics/exceptions.h"

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

NetworkAttachedFollower::NetworkAttachedFollower(network::ConnectionPool *pool,
                                                 ParticipantId id,
                                                 DatabaseID database,
                                                 log_id log_id) :
    pool(pool),
    id(std::move(id)), database(std::move(database)), log_id(log_id) {
}

auto NetworkAttachedFollower::getParticipantId() const noexcept -> ParticipantId const & {
    return id;
}

auto NetworkAttachedFollower::appendEntries(AppendEntriesRequest request) -> futures::Future<AppendEntriesResult> {
    VPackBufferUInt8 buffer;
    {
        VPackBuilder builder(buffer);
        request.to_velocy_pack(builder);
    }

    auto path = basics::StringUtils::joinT("/", StaticStrings::ApiLogInternal, log_id, "append-entries");
    network::RequestOptions opts;
    opts.database = database;
    auto f =
        network::sendRequest(pool, "server:" + id, nil::dbms::fuerte::RestVerb::Post, path, std::move(buffer), opts);

    return std::move(f).thenValue([](network::Response result) -> AppendEntriesResult {
        if (result.fail() || !fuerte::statusIsSuccess(result.statusCode())) {
            THROW_DBMS_EXCEPTION(result.combinedResult());
        }
        TRI_ASSERT(result.slice().get("error").isFalse());
        return AppendEntriesResult::from_velocy_pack(result.slice().get("result"));
    });
}
