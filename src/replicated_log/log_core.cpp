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

#include <nil/replication_sdk/replicated_log/log_core.hpp>
#include <nil/replication_sdk/replicated_log/persisted_log.hpp>

#include <basics/exceptions.h>
#include <basics/debugging.h>
#include <basics/system_compiler.h>
#include <basics/voc_errors.h>

#include <utility>

using namespace nil::dbms;
using namespace nil::dbms::replication_sdk;
using namespace nil::dbms::replication_sdk::replicated_log;

replicated_log::log_core::log_core(std::shared_ptr<persisted_log> persistedLog) : _persistedLog(std::move(persistedLog)) {
    if (ADB_UNLIKELY(_persistedLog == nullptr)) {
        TRI_ASSERT(false);
        THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_INTERNAL,
                                     "When instantiating ReplicatedLog: "
                                     "persistedLog must not be a nullptr");
    }
}

auto replicated_log::log_core::remove_back(log_index first) -> Result {
    std::unique_lock guard(_operationMutex);
    return _persistedLog->removeBack(first);
}

auto replicated_log::log_core::insert(persisted_log_iterator &iter, bool waitForSync) -> Result {
    std::unique_lock guard(_operationMutex);
    persisted_log::write_options opts;
    opts.waitForSync = waitForSync;
    return _persistedLog->insert(iter, opts);
}

auto replicated_log::log_core::read(log_index first) const -> std::unique_ptr<persisted_log_iterator> {
    std::unique_lock guard(_operationMutex);
    return _persistedLog->read(first);
}

auto replicated_log::log_core::insert_async(std::unique_ptr<persisted_log_iterator> iter, bool waitForSync)
    -> futures::Future<Result> {
    std::unique_lock guard(_operationMutex);
    // This will hold the mutex
    persisted_log::write_options opts;
    opts.waitForSync = waitForSync;
    return _persistedLog->insertAsync(std::move(iter), opts)
        .thenValue([guard = std::move(guard)](Result &&res) mutable {
            guard.unlock();
            return std::move(res);
        });
}

auto replicated_log::log_core::release_persisted_log() && -> std::shared_ptr<persisted_log> {
    std::unique_lock guard(_operationMutex);
    return std::move(_persistedLog);
}

auto replicated_log::log_core::log_id() const noexcept -> LogId {
    return _persistedLog->id();
}

auto log_core::remove_front(log_index stop) -> futures::Future<Result> {
    std::unique_lock guard(_operationMutex);
    return _persistedLog->removeFront(stop).thenValue([guard = std::move(guard)](Result &&res) mutable {
        guard.unlock();
        return std::move(res);
    });
}

auto log_core::gid() const noexcept -> global_log_identifier const & {
    return _persistedLog->gid();
}
