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

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

struct TRI_vocbase_t;

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
    class LogId;

    /**
     * This is a collection of functions that is used by the RestHandler. It covers
     * two different implementations. One for dbservers that actually execute the
     * commands and one for coordinators that forward the request to the leader.
     */
    struct prototype_state_methods {
        virtual ~prototype_state_methods() = default;

        struct create_options {
            bool waitForReady {false};
            std::optional<LogId> id;
            std::optional<agency::log_target_config> config;
            std::vector<ParticipantId> servers;
        };

        struct create_result {
            LogId id;
            std::vector<ParticipantId> servers;
        };

        struct prototype_write_options {
            bool waitForCommit {true};
            bool waitForSync {false};
            bool waitForApplied {true};
        };

        [[nodiscard]] virtual auto createState(create_options options) const
            -> futures::Future<ResultT<create_result>> = 0;

        [[nodiscard]] virtual auto insert(LogId id, std::unordered_map<std::string, std::string> const &entries,
                                          prototype_write_options) const -> futures::Future<log_index> = 0;

        [[nodiscard]] virtual auto compareExchange(LogId id, std::string key, std::string oldValue,
                                                   std::string newValue, prototype_write_options) const
            -> futures::Future<ResultT<log_index>> = 0;

        [[nodiscard]] virtual auto get(LogId id, std::string key, log_index waitForApplied) const
            -> futures::Future<ResultT<std::optional<std::string>>>;
        [[nodiscard]] virtual auto get(LogId id, std::vector<std::string> keys, log_index waitForApplied) const
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>>;

        struct ReadOptions {
            log_index waitForApplied {0};
            std::optional<ParticipantId> readFrom {};
        };

        [[nodiscard]] virtual auto get(LogId id, std::string key, ReadOptions const &) const
            -> futures::Future<ResultT<std::optional<std::string>>>;

        [[nodiscard]] virtual auto get(LogId id, std::vector<std::string> keys, ReadOptions const &) const
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> = 0;

        [[nodiscard]] virtual auto getSnapshot(LogId id, log_index waitForIndex) const
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> = 0;

        [[nodiscard]] virtual auto remove(LogId id, std::string key, prototype_write_options) const
            -> futures::Future<log_index> = 0;
        [[nodiscard]] virtual auto remove(LogId id, std::vector<std::string> keys, prototype_write_options) const
            -> futures::Future<log_index> = 0;

        virtual auto waitForApplied(LogId id, log_index waitForIndex) const -> futures::Future<Result> = 0;
        virtual auto drop(LogId id) const -> futures::Future<Result> = 0;

        struct PrototypeStatus {
            // TODO
            LogId id;
        };

        [[nodiscard]] virtual auto status(LogId) const -> futures::Future<ResultT<PrototypeStatus>> = 0;

        static auto createInstance(TRI_vocbase_t &vocbase) -> std::shared_ptr<prototype_state_methods>;
    };

    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::create_options &x) {
        return f.object(x).fields(f.field("waitForReady", x.waitForReady).fallback(true), f.field("id", x.id),
                                  f.field("config", x.config),
                                  f.field("servers", x.servers).fallback(std::vector<ParticipantId> {}));
    }
    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::create_result &x) {
        return f.object(x).fields(f.field("id", x.id), f.field("servers", x.servers));
    }
    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::PrototypeStatus &x) {
        return f.object(x).fields(f.field("id", x.id));
    }
    template<class Inspector>
    auto inspect(Inspector &f, prototype_state_methods::prototype_write_options &x) {
        return f.object(x).fields(f.field("waitForCommit", x.waitForCommit).fallback(true),
                                  f.field("waitForSync", x.waitForSync).fallback(false),
                                  f.field("waitForApplied", x.waitForApplied).fallback(true));
    }
}    // namespace nil::dbms::replication
