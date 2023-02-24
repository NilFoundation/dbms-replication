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

#define BOOST_AUTO_TEST_MAIN

#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

#include <array>
#include <utility>

#include "../replicated_log/test_helper.hpp"

#include <nil/dbms/replication/replicated_state/replicated_state.hpp>
#include <nil/dbms/replication/replicated_state/replicated_state_feature.hpp>

#include <nil/dbms/replication/state_machines/prototype/prototype_core.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_follower_state.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_leader_state.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_machine.hpp>
#include <nil/dbms/replication/state_machines/prototype/prototype_state_methods.hpp>

using namespace nil::dbms;
using namespace nil::dbms::replication;
using namespace nil::dbms::replication::replicated_state;
using namespace nil::dbms::replication::replicated_state::prototype;
using namespace nil::dbms::replication::test;

#include <nil/dbms/replication/replicated_state/replicated_state.tpp>
#include "../mocks/mock_state_persistor_interface.hpp"

namespace std {
    template<typename T>
    std::ostream &operator<<(std::ostream &os, const std::optional<T> &opt) {
        return opt ? os << opt.value() : os << "std::nullopt";
    }

    template<typename T1, typename T2>
    std::ostream &operator<<(std::ostream &os, const std::unordered_map<T1, T2> &opt) {
        std::size_t i = 0;
        for (const auto &p : opt) {
            os << "element " << i << ": key: " << p.first << " value: " << p.second << std::endl;
            i++;
        }
        return os;
    }

    std::ostream &operator<<(std::ostream &os, const std::nullopt_t opt) {
        return os << "std::nullopt";
    }

}    // namespace std

namespace {
    struct mock_prototype_leader_interface : public iprototype_leader_interface {
        explicit mock_prototype_leader_interface(std::shared_ptr<prototype_leader_state> leaderState,
                                              bool useDefaultSnapshot) :
            leaderState(std::move(leaderState)),
            useDefaultSnapshot(useDefaultSnapshot), defaultSnapshot {{"a", "b"}, {"c", "d"}} {
        }

        auto getSnapshot(global_log_identifier const &, log_index waitForIndex)
            -> futures::Future<ResultT<std::unordered_map<std::string, std::string>>> override {
            if (useDefaultSnapshot) {
                return ResultT<std::unordered_map<std::string, std::string>>::success(defaultSnapshot);
            }
            return leaderState->getSnapshot(waitForIndex);
        }

        std::shared_ptr<prototype_leader_state> leaderState;
        bool useDefaultSnapshot;
        std::unordered_map<std::string, std::string> defaultSnapshot;
    };

    struct mock_prototype_network_interface : public iprototype_network_interface {
        auto getLeaderInterface(ParticipantId id) -> ResultT<std::shared_ptr<iprototype_leader_interface>> override {
            if (auto leaderState = leaderStates.find(id); leaderState != leaderStates.end()) {
                return ResultT<std::shared_ptr<iprototype_leader_interface>>::success(
                    std::make_shared<mock_prototype_leader_interface>(leaderState->second, useDefaultSnapshot));
            }
            return {TRI_ERROR_CLUSTER_NOT_LEADER};
        }

        void addLeaderState(ParticipantId id, std::shared_ptr<prototype_leader_state> leaderState) {
            leaderStates.emplace(std::move(id), std::move(leaderState));
        }

        bool useDefaultSnapshot = false;
        std::unordered_map<ParticipantId, std::shared_ptr<prototype_leader_state>> leaderStates;
    };

    struct mock_prototype_storage_interface : public iprototype_storage_interface {
        auto put(const global_log_identifier &logId, prototype_dump dump) -> Result override {
            map[logId.id] = std::move(dump);
            ++putCalled;
            return TRI_ERROR_NO_ERROR;
        }

        auto get(const global_log_identifier &logId) -> ResultT<prototype_dump> override {
            if (!map.contains(logId.id)) {
                return ResultT<prototype_dump>::success(map[logId.id] = prototype_dump {});
            }
            return ResultT<prototype_dump>::success(map[logId.id]);
        }

        std::unordered_map<LogId, prototype_dump> map;
        int putCalled {0};
    };
}    // namespace

struct prototype_state_machine_test : test::replicated_log_test {
    prototype_state_machine_test() {
        feature->registerStateType<prototype::prototype_state>("prototype-state", networkMock, storageMock);
    }

    std::shared_ptr<replicated_state_feature> feature = std::make_shared<replicated_state_feature>();
    std::shared_ptr<mock_prototype_network_interface> networkMock = std::make_shared<mock_prototype_network_interface>();
    std::shared_ptr<mock_prototype_storage_interface> storageMock = std::make_shared<mock_prototype_storage_interface>();
    std::shared_ptr<mock_state_persistor_interface> statePersistor = std::make_shared<mock_state_persistor_interface>();
};

BOOST_FIXTURE_TEST_CASE(prototype_core_flush, prototype_state_machine_test) {
    auto logId = LogId {1};
    auto followerLog = makeReplicatedLog(logId);
    auto follower = followerLog->become_follower("follower", log_term {1}, "leader");

    auto leaderLog = makeReplicatedLog(logId);
    auto leader = leaderLog->become_leader("leader", log_term {1}, {follower}, 2);

    leader->triggerAsyncReplication();

    auto leaderReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", leaderLog));
    BOOST_CHECK_NE(leaderReplicatedState, nullptr);
    leaderReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));
    follower->runAllAsyncAppendEntries();

    auto leaderState = leaderReplicatedState->getLeader();
    BOOST_CHECK_NE(leaderState, nullptr);
    networkMock->addLeaderState("leader", leaderState);

    auto followerReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", followerLog));
    BOOST_CHECK_NE(followerReplicatedState, nullptr);
    followerReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));

    auto followerState = followerReplicatedState->getFollower();
    BOOST_CHECK_NE(followerState, nullptr);

    std::unordered_map<std::string, std::string> expected;
    for (std::size_t cnt {0}; cnt < prototype_core::kFlushBatchSize; ++cnt) {
        auto key = "foo" + std::to_string(cnt);
        auto value = "bar" + std::to_string(cnt);
        auto entries = std::unordered_map<std::string, std::string> {{key, value}};
        expected.emplace(key, value);
        auto result =
            leaderState->set(entries, prototype_state_methods::prototype_write_options {.waitForApplied = false});
        BOOST_ASSERT(result.isReady());
        auto index = result.get().value;
        BOOST_CHECK_EQUAL(index, cnt + 2);
    }
    follower->runAllAsyncAppendEntries();

    // put is called twice, once from the leader and once from the follower
    BOOST_CHECK_EQUAL(storageMock->putCalled, 2);

    auto snapshot = leaderState->getSnapshot(log_index {1});
    BOOST_ASSERT(snapshot.isReady());
    auto leaderMap = snapshot.get().get();
    BOOST_CHECK_EQUAL(expected, leaderMap);

    auto prototypeDump = storageMock->get(global_log_identifier {"database", logId});
    BOOST_CHECK_EQUAL(prototypeDump.get().map, leaderMap);
}

BOOST_FIXTURE_TEST_CASE(simple_operations, prototype_state_machine_test) {
    auto followerLog = makeReplicatedLog(LogId {1});
    auto follower = followerLog->become_follower("follower", log_term {1}, "leader");

    auto leaderLog = makeReplicatedLog(LogId {1});
    auto leader = leaderLog->become_leader("leader", log_term {1}, {follower}, 2);

    leader->triggerAsyncReplication();

    auto leaderReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", leaderLog));
    BOOST_CHECK_NE(leaderReplicatedState, nullptr);
    leaderReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));
    follower->runAllAsyncAppendEntries();

    auto leaderState = leaderReplicatedState->getLeader();
    BOOST_CHECK_NE(leaderState, nullptr);
    networkMock->addLeaderState("leader", leaderState);

    auto followerReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", followerLog));
    BOOST_CHECK_NE(followerReplicatedState, nullptr);
    followerReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));

    auto followerState = followerReplicatedState->getFollower();
    BOOST_CHECK_NE(followerState, nullptr);

    decltype(log_index::value) index {0};
    prototype_state_methods::prototype_write_options options {};

    // Compare-exchange before insert
    {
        auto result = leaderState->compareExchange("cmp", "cmp1", "cmp2", options).get();
        BOOST_CHECK_EQUAL(result.errorNumber(), TRI_ERROR_DBMS_CONFLICT);
    }

    // Get before insert
    {
        auto result = leaderState->get("baz", log_index {index}).get();
        BOOST_CHECK_EQUAL(result.get(), std::nullopt);
        result = followerState->get("baz", log_index {index}).get();
        BOOST_CHECK_EQUAL(result.get(), std::nullopt);
    }

    // Inserting one entry
    {
        auto entries = std::unordered_map<std::string, std::string> {{"foo", "bar"}};
        auto result = leaderState->set(std::move(entries), options);
        follower->runAllAsyncAppendEntries();
        index = result.get().value;
        BOOST_CHECK_EQUAL(index, 2);
    }

    // Single get
    {
        auto result = leaderState->get("foo", log_index {index}).get();
        BOOST_CHECK_EQUAL(result.get(), "bar");
        result = leaderState->get("baz", log_index {index}).get();
        BOOST_CHECK_EQUAL(result.get(), std::nullopt);

        result = followerState->get("foo", log_index {index}).get();
        BOOST_CHECK_EQUAL(result.get(), "bar");
        result = followerState->get("baz", log_index {index}).get();
        BOOST_CHECK_EQUAL(result.get(), std::nullopt);
    }

    // Inserting multiple entries
    {
        std::unordered_map<std::string, std::string> entries {{"foo1", "bar1"}, {"foo2", "bar2"}, {"foo3", "bar3"}};
        auto result = leaderState->set(std::move(entries), options);
        follower->runAllAsyncAppendEntries();
        BOOST_ASSERT(result.isReady());
        index = result.get().value;
        BOOST_CHECK_EQUAL(index, 3);
    }

    // Getting multiple entries
    {
        std::vector<std::string> entries = {"foo1", "foo2", "foo3", "nofoo"};
        std::unordered_map<std::string, std::string> result = leaderState->get(entries, log_index {index}).get().get();
        BOOST_CHECK_EQUAL(result.size(), 3);
        BOOST_CHECK_EQUAL(result["foo1"], "bar1");
        BOOST_CHECK_EQUAL(result["foo2"], "bar2");
        BOOST_CHECK_EQUAL(result["foo3"], "bar3");

        result = followerState->get(entries, log_index {index}).get().get();
        BOOST_CHECK_EQUAL(result.size(), 3);
        BOOST_CHECK_EQUAL(result["foo1"], "bar1");
        BOOST_CHECK_EQUAL(result["foo2"], "bar2");
        BOOST_CHECK_EQUAL(result["foo3"], "bar3");
    }

    // Removing single entry
    {
        auto result = leaderState->remove("foo1", options);
        follower->runAllAsyncAppendEntries();
        BOOST_ASSERT(result.isReady());
        index = result.get().value;
        BOOST_CHECK_EQUAL(index, 4);
        BOOST_CHECK_EQUAL(leaderState->get("foo1", log_index {index}).get().get(), std::nullopt);
    }

    // Removing multiple entries
    {
        std::vector<std::string> entries = {"nofoo", "foo2"};
        auto result = leaderState->remove(std::move(entries), options);
        follower->runAllAsyncAppendEntries();
        BOOST_ASSERT(result.isReady());
        index = result.get().value;
        BOOST_CHECK_EQUAL(index, 5);
        BOOST_CHECK_EQUAL(leaderState->get("foo2", log_index {index}).get().get(), std::nullopt);
        BOOST_CHECK_EQUAL(leaderState->get("foo3", log_index {index}).get().get(), "bar3");
        BOOST_CHECK_EQUAL(followerState->get("foo2", log_index {index}).get().get(), std::nullopt);
        BOOST_CHECK_EQUAL(followerState->get("foo3", log_index {index}).get().get(), "bar3");
    }

    // Compare Exchange
    {
        auto wrongValue = leaderState->compareExchange("foo3", "foobar", "foobar", options).get();
        BOOST_CHECK_EQUAL(wrongValue.errorNumber(), TRI_ERROR_DBMS_CONFLICT);
        auto result = leaderState->compareExchange("foo3", "bar3", "foobar", options);
        follower->runAllAsyncAppendEntries();
        BOOST_ASSERT(result.isReady());
        index = result.get().get().value;
        BOOST_CHECK_EQUAL(index, 6);
    }

    // Check final state
    {
        auto result = leaderState->getSnapshot(log_index {3});
        BOOST_ASSERT(result.isReady());
        auto map = result.get();
        auto expected = std::unordered_map<std::string, std::string> {{"foo", "bar"}, {"foo3", "foobar"}};
        //    TODO: return this
        //    BOOST_CHECK_EQUAL(map, expected);
        BOOST_CHECK_EQUAL(followerState->get("foo", log_index {index}).get().get(), "bar");
        BOOST_CHECK_EQUAL(followerState->get("foo3", log_index {index}).get().get(), "foobar");
    }
}

BOOST_FIXTURE_TEST_CASE(snapshot_transfer, prototype_state_machine_test) {
    networkMock->useDefaultSnapshot = true;
    auto logId = LogId {1};
    auto followerLog = makeReplicatedLog(logId);
    auto follower = followerLog->become_follower("follower", log_term {1}, "leader");

    auto leaderLog = makeReplicatedLog(logId);
    auto leader = leaderLog->become_leader("leader", log_term {1}, {follower}, 2);

    leader->triggerAsyncReplication();

    auto leaderReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", leaderLog));
    BOOST_CHECK_NE(leaderReplicatedState, nullptr);
    leaderReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));
    follower->runAllAsyncAppendEntries();

    auto leaderState = leaderReplicatedState->getLeader();
    BOOST_CHECK_NE(leaderState, nullptr);
    networkMock->addLeaderState("leader", leaderState);

    auto followerReplicatedState = std::dynamic_pointer_cast<replicated_state_t<prototype_state>>(
        feature->create_replicated_state("prototype-state", followerLog));
    BOOST_CHECK_NE(followerReplicatedState, nullptr);
    followerReplicatedState->start(std::make_unique<replicated_state_token>(state_generation {1}));

    auto followerState = followerReplicatedState->getFollower();
    BOOST_CHECK_NE(followerState, nullptr);
    BOOST_CHECK_EQUAL(followerState->get("a", log_index {0}).get().get(), "b");
    BOOST_CHECK_EQUAL(followerState->get("c", log_index {0}).get().get(), "d");
}
