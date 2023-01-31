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

#include "nil/dbms/vocbase/vocbase.hpp"

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace nil::dbms {

    template<typename T>
    class ResultT;

    class Result;

    namespace futures {
        template<typename T>
        class Future;
    }
}    // namespace nil::dbms

namespace nil::dbms::replication {
    class log_id;

    /**
     * This is a collection of functions that is used by the RestHandler. It covers
     * two different implementations. One for dbservers that actually execute the
     * commands and one for coordinators that forward the request to the leader.
     */
    struct prototype_state_methods {
        virtual ~prototype_state_methods() = default;

        struct CreateOptions {
            bool waitForReady {false};
            std::optional<log_id> id;
            std::optional<agency::LogTargetConfig> config;
            std::vector<ParticipantId> servers;
        };

        struct CreateResult {
            log_id id;
            std::vector<ParticipantId> servers;
        };

        struct PrototypeWriteOptions {
            bool waitForCommit {true};
            bool waitForSync {false};
            bool wait_for_applied {true};
        };

        [[nodiscard]] virtual auto createState(CreateOptions options) const
            -> futures::Future<ResultT<CreateResult>> = 0;

        [[nodiscard]] virtual auto insert(log_id id, std::unordered_map<std::string, std::string> const &entries,
                                          PrototypeWriteOptions) const -> futures::Future<log_index> = 0;

        [[nodiscard]] virtual auto compare_exchange(log_id id, std::string key, std::string oldValue,
                                                   std::string newValue, PrototypeWriteOptions) const
            -> futures::Future<ResultT<log_index>> = 0;

        [[nodiscard]] virtual auto get(log_id id, std::string key, log_index wait_for_applied) const
            -> futures::Future<ResultT<std::optional<std::string>>>;
        [[nodiscard]] virtual auto get(log_id id, std::vector<std::string> keys, log_index wait_for_applied) const
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        struct ReadOptions {
            log_index wait_for_applied {0};
            std::optional<ParticipantId> readFrom {};
        };

        [[nodiscard]] virtual auto get(log_id id, std::string key, ReadOptions const &) const
            -> futures::Future<ResultT<std::optional<std::string>>>;

        [[nodiscard]] virtual auto get(log_id id, std::vector<std::string> keys, ReadOptions const &) const
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> = 0;

        [[nodiscard]] virtual auto get_snapshot(log_id id, log_index waitForIndex) const
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> = 0;

        [[nodiscard]] virtual auto remove(log_id id, std::string key, PrototypeWriteOptions) const
            -> futures::Future<log_index> = 0;
        [[nodiscard]] virtual auto remove(log_id id, std::vector<std::string> keys, PrototypeWriteOptions) const
            -> futures::Future<log_index> = 0;

        virtual auto wait_for_applied(log_id id, log_index waitForIndex) const -> futures::Future<Result> = 0;
        virtual auto drop(log_id id) const -> futures::Future<Result> = 0;

        struct PrototypeStatus {
            // TODO
            log_id id;
        };

        [[nodiscard]] virtual auto status(log_id) const -> futures::Future<ResultT<PrototypeStatus>> = 0;

        static auto create_instance(TRI_vocbase_t &vocbase) -> std::shared_ptr<prototype_state_methods>;
    };

    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::CreateOptions &x) {
        return f.object(x).fields(f.field("waitForReady", x.waitForReady).fallback(true), f.field("id", x.id),
                                  f.field("config", x.config),
                                  f.field("servers", x.servers).fallback(std::vector<ParticipantId> {}));
    }
    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::CreateResult &x) {
        return f.object(x).fields(f.field("id", x.id), f.field("servers", x.servers));
    }
    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::PrototypeStatus &x) {
        return f.object(x).fields(f.field("id", x.id));
    }
    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::PrototypeWriteOptions &x) {
        return f.object(x).fields(f.field("waitForCommit", x.waitForCommit).fallback(true),
                                  f.field("waitForSync", x.waitForSync).fallback(false),
                                  f.field("wait_for_applied", x.wait_for_applied).fallback(true));
    }
}    // namespace nil::dbms::replication
