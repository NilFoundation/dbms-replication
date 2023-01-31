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

#include <nil/dbms/replication/log/log_core.hpp>
#include <nil/dbms/replication/log/persisted_log.hpp>

#include <basics/exceptions.h>
#include <basics/debugging.h>
#include <basics/system_compiler.h>
#include <basics/voc_errors.h>

#include <utility>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::log;

log::log_core::log_core(std::shared_ptr<persisted_log> persisted_log) : _persisted_log(std::move(persisted_log)) {
    if (ADB_UNLIKELY(_persisted_log == nullptr)) {
        TRI_ASSERT(false);
        THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                     "When instantiating ReplicatedLog: "
                                     "persisted_log must not be a nullptr");
    }
}

auto log::log_core::remove_back(log_index first) -> Result {
    std::unique_lock guard(_operationMutex);
    return _persisted_log->remove_back(first);
}

auto log::log_core::insert(persisted_logIterator &iter, bool waitForSync) -> Result {
    std::unique_lock guard(_operationMutex);
    persisted_log::WriteOptions opts;
    opts.waitForSync = waitForSync;
    return _persisted_log->insert(iter, opts);
}

auto log::log_core::read(log_index first) const -> std::unique_ptr<persisted_logIterator> {
    std::unique_lock guard(_operationMutex);
    return _persisted_log->read(first);
}

auto log::log_core::insert_async(std::unique_ptr<persisted_logIterator> iter, bool waitForSync)
    -> futures::Future<Result> {
    std::unique_lock guard(_operationMutex);
    // This will hold the mutex
    persisted_log::WriteOptions opts;
    opts.waitForSync = waitForSync;
    return _persisted_log->insert_async(std::move(iter), opts)
        .thenValue([guard = std::move(guard)](Result &&res) mutable {
            guard.unlock();
            return std::move(res);
        });
}

auto log::log_core::releasepersisted_log() && -> std::shared_ptr<persisted_log> {
    std::unique_lock guard(_operationMutex);
    return std::move(_persisted_log);
}

auto log::log_core::log_id() const noexcept -> log_id {
    return _persisted_log->id();
}

auto log_core::remove_front(log_index stop) -> futures::Future<Result> {
    std::unique_lock guard(_operationMutex);
    return _persisted_log->remove_front(stop).thenValue([guard = std::move(guard)](Result &&res) mutable {
        guard.unlock();
        return std::move(res);
    });
}

auto log_core::gid() const noexcept -> global_log_identifier const & {
    return _persisted_log->gid();
}
