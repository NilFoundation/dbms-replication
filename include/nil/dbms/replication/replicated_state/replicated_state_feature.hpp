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
#include <unordered_map>
#include <utility>

#include "replicated_state.hpp"
#include "replicated_state_traits.hpp"
#include <nil/dbms/replication/dbmsd/dbmsd.hpp>

namespace nil::dbms::replication::replicated_log {
    struct replicated_log_t;
    class log_follower;
    class log_leader;
}    // namespace nil::dbms::replication::replicated_log

namespace nil::dbms::replication::replicated_state {

    struct replicated_state_feature {
        /**
         * Registers a new State implementation with the given name.
         * @tparam S State Machine
         * @param name Name of the implementation
         * @param args Arguments forwarded to the constructor of the factory type,
         * i.e. ReplicatedStateTraits<S>::FactoryType.
         */
        template<typename S, typename... Args>
        void registerStateType(std::string name, Args &&...args) {
            using Factory = typename replicated_state_traits<S>::FactoryType;
            static_assert(std::is_constructible<Factory, Args...>::value);
            auto factory = std::make_shared<InternalFactory<S, Factory>>(std::in_place, std::forward<Args>(args)...);
            auto [iter, wasInserted] = factories.try_emplace(std::move(name), std::move(factory));
            assertWasInserted(name, wasInserted);
        }

        /**
         * Create a new replicated state using the implementation specified by the
         * name.
         * @param name Which implementation to use.
         * @param log ReplicatedLog to use.
         * @return
         */
        auto createReplicatedState(std::string_view name, std::shared_ptr<replicated_log::replicated_log_t> log)
            -> std::shared_ptr<replicated_state_base>;

        auto createReplicatedState(std::string_view name,
                                   std::shared_ptr<replicated_log::replicated_log_t>
                                       log,
                                   logger_context const &) -> std::shared_ptr<replicated_state_base>;

        template<typename S>
        auto createReplicatedStateAs(std::string_view name, std::shared_ptr<replicated_log::replicated_log_t> log)
            -> std::shared_ptr<replicated_state_t<S>> {
            return std::dynamic_pointer_cast<replicated_state_t<S>>(createReplicatedState(name, std::move(log)));
        }

    private:
        static void assertWasInserted(std::string_view name, bool wasInserted);
        struct InternalFactoryBase : std::enable_shared_from_this<InternalFactoryBase> {
            virtual ~InternalFactoryBase() = default;
            virtual auto create_replicated_state(std::shared_ptr<replicated_log::replicated_log_t>, logger_context)
                -> std::shared_ptr<replicated_state_base> = 0;
        };

        template<typename S, typename Factory>
        struct InternalFactory;

        std::unordered_map<std::string, std::shared_ptr<InternalFactoryBase>> factories;
    };

    template<typename S, typename Factory = typename replicated_state_traits<S>::FactoryType>
    struct replicated_state_feature::InternalFactory : InternalFactoryBase, private Factory {
        template<typename... Args>
        explicit InternalFactory(std::in_place_t, Args &&...args) : Factory(std::forward<Args>(args)...) {
        }

        auto create_replicated_state(std::shared_ptr<replicated_log::replicated_log_t> log,
                                     logger_context loggerContext) -> std::shared_ptr<replicated_state_base> override {
            return std::make_shared<replicated_state_t<S>>(std::move(log), getStateFactory(), std::move(loggerContext));
        }

        auto getStateFactory() -> std::shared_ptr<Factory> {
            return {shared_from_this(), static_cast<Factory *>(this)};
        }
    };

    struct replicated_state_app_feature : DbmsdFeature, replicated_state_feature {
        constexpr static const char *name() noexcept {
            return "replicated_state";
        }

        explicit replicated_state_app_feature(Server &server);
    };

}    // namespace nil::dbms::replication::replicated_state
