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

#include "log_common.hpp"
#include "log_entries.hpp"

#include "basics/result.h"
#include "futures/Future.h"

#include <memory>
#include <utility>

namespace nil::dbms::replication::replicated_log {

    /**
     * @brief Interface to persist a replicated log locally. Implemented by
     * RocksDBLog.
     */
    struct persisted_log {
        virtual ~persisted_log() = default;
        explicit persisted_log(global_log_identifier gid) : _gid(std::move(gid)) {
        }

        struct write_options {
            bool waitForSync = false;
        };

        [[nodiscard]] auto id() const noexcept -> LogId {
            return _gid.id;
        }
        [[nodiscard]] auto gid() const noexcept -> global_log_identifier const & {
            return _gid;
        }
        virtual auto insert(persisted_log_iterator &iter, write_options const &) -> Result = 0;
        virtual auto insertAsync(std::unique_ptr<persisted_log_iterator> iter, write_options const &)
            -> futures::Future<Result> = 0;
        virtual auto read(log_index start) -> std::unique_ptr<persisted_log_iterator> = 0;
        virtual auto removeFront(log_index stop) -> futures::Future<Result> = 0;
        virtual auto removeBack(log_index start) -> Result = 0;

        virtual auto drop() -> Result = 0;

    private:
        global_log_identifier _gid;
    };

}    // namespace nil::dbms::replication::replicated_log
