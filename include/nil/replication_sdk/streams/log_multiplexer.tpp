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

#include <map>
#include <memory>
#include <tuple>
#include <type_traits>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// conversion from 'size_t' to 'immer::detail::rbts::count_t', possible loss of
// data
#pragma warning(disable : 4267)
// result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift
// intended?)
#pragma warning(disable : 4334)
#endif
#include <immer/flex_vector.hpp>
#include <immer/flex_vector_transient.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

#include <basics/exceptions.h>
#include <basics/guarded.h>
#include <basics/unshackled_mutex.h>
#include <basics/application_exit.h>

#include "log_multiplexer.hpp"
#include <nil/replication_sdk/exceptions/participant_resigned_exception.hpp>

#include <nil/replication_sdk/replicated_log/ilog_interfaces.hpp>

#include <nil/replication_sdk/streams/multiplexed_values.hpp>
#include <nil/replication_sdk/streams/stream_information_block.hpp>
#include "streams.hpp"
#include <nil/replication_sdk/streams/stream_information_block.tpp>
#include <nil/replication_sdk/streams/streams.tpp>

namespace nil::dbms::replication_sdk::streams {

    namespace {
        template<typename Queue, typename Result>
        auto all_unresolved(std::pair<Queue, Result> &q) {
            return std::all_of(std::begin(q.first), std::end(q.first),
                               [&](auto const &pair) { return !pair.second.isFulfilled(); });
        }
        template<typename Descriptor, typename Queue, typename Result,
                 typename Block = stream_information_block<Descriptor>>
        auto resolve_promise_set(std::pair<Queue, Result> &q) {
            TRI_ASSERT(all_unresolved(q));
            std::for_each(std::begin(q.first), std::end(q.first), [&](auto &pair) {
                TRI_ASSERT(!pair.second.isFulfilled());
                if (!pair.second.isFulfilled()) {
                    pair.second.setTry(std::move(q.second));
                }
            });
        }

        template<typename... Descriptors, typename... Pairs, std::size_t... Idxs>
        auto resolve_promise_sets(stream_descriptor_set<Descriptors...>,
                                std::index_sequence<Idxs...>,
                                std::tuple<Pairs...> &pairs) {
            (resolve_promise_set<Descriptors>(std::get<Idxs>(pairs)), ...);
        }

        template<typename... Descriptors, typename... Pairs>
        auto resolve_promise_sets(stream_descriptor_set<Descriptors...>, std::tuple<Pairs...> &pairs) {
            resolve_promise_sets(stream_descriptor_set<Descriptors...> {}, std::index_sequence_for<Descriptors...> {},
                                 pairs);
        }
    }    // namespace

    template<typename Derived, typename Spec, template<typename> typename StreamInterface>
    struct log_multiplexer_implementation_base {
        explicit log_multiplexer_implementation_base() : _guardedData(static_cast<Derived &>(*this)) {
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>,
                 typename E = StreamEntryView<T>>
        auto wait_for_iterator_internal(log_index first) -> futures::Future<std::unique_ptr<typed_log_range_iterator<E>>> {
            return wait_for_internal<StreamDescriptor>(first).thenValue(
                [weak = weak_from_self(), first](auto &&) -> std::unique_ptr<typed_log_range_iterator<E>> {
                    if (auto that = weak.lock(); that != nullptr) {
                        return that->_guardedData.doUnderLock([&](multiplexer_data<Spec> &self) {
                            auto &block = std::get<stream_information_block<StreamDescriptor>>(self._blocks);
                            return block.get_iterator_range(first, self._firstUncommittedIndex);
                        });
                    } else {
                        return nullptr;
                    }
                });
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>,
                 typename W = typename Stream<T>::wait_for_result>
        auto wait_for_internal(log_index index) -> futures::Future<W> {
            return _guardedData.doUnderLock([&](multiplexer_data<Spec> &self) {
                if (self._firstUncommittedIndex > index) {
                    return futures::Future<W> {std::in_place};
                }
                auto &block = std::get<stream_information_block<StreamDescriptor>>(self._blocks);
                return block.register_wait_for(index);
            });
        }

        template<typename StreamDescriptor>
        auto release_internal(log_index index) -> void {
            // update the release index for the given stream
            // then compute the minimum and forward it to the
            // actual log implementation
            auto globalReleaseIndex =
                _guardedData.doUnderLock([&](multiplexer_data<Spec> &self) -> std::optional<log_index> {
                    {
                        auto &block = self.template get_block_for_descriptor<StreamDescriptor>();
                        auto newIndex = std::max(block._releaseIndex, index);
                        if (newIndex == block._releaseIndex) {
                            return std::nullopt;
                        }
                        TRI_ASSERT(newIndex > block._releaseIndex);
                        block._releaseIndex = newIndex;
                    }

                    return self.min_release_index();
                });

            if (globalReleaseIndex) {
                // TODO handle return value
                std::ignore = get_log_interface()->release(*globalReleaseIndex);
            }
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>,
                 typename E = StreamEntryView<T>>
        auto get_iterator_internal() -> std::unique_ptr<typed_log_range_iterator<E>> {
            return _guardedData.doUnderLock([](multiplexer_data<Spec> &self) {
                auto &block = self.template get_block_for_descriptor<StreamDescriptor>();
                return block.get_iterator();
            });
        }

        auto resolve_leader_change(std::exception_ptr ptr) {
            auto promiseSet = _guardedData.getLockedGuard()->get_change_leader_resolve_set(ptr);
            resolve_promise_sets(Spec {}, promiseSet);
        }

    protected:
        auto &get_log_interface() {
            return static_cast<Derived &>(*this)._interface;
        }

        template<typename>
        struct multiplexer_data;
        template<typename... Descriptors>
        struct multiplexer_data<stream_descriptor_set<Descriptors...>> {
            std::tuple<stream_information_block<Descriptors>...> _blocks;
            log_index _firstUncommittedIndex {1};
            log_index _lastIndex;
            bool _pendingWaitFor {false};

            Derived &_self;

            explicit multiplexer_data(Derived &self) : _self(self) {
            }
            void digest_iterator(LogIterator &iter) {
                while (auto memtry = iter.next()) {
                    auto muxedValue = multiplexed_values::fromVelocyPack<Descriptors...>(memtry->logPayload());
                    std::visit(
                        [&](auto &&value) {
                            using ValueTag = std::decay_t<decltype(value)>;
                            using Descriptor = typename ValueTag::DescriptorType;
                            std::get<stream_information_block<Descriptor>>(_blocks).append_entry(
                                memtry->logIndex(), std::move(value.value));
                        },
                        std::move(muxedValue.variant()));
                }
            }

            auto get_change_leader_resolve_set(std::exception_ptr ptr) {
                return std::make_tuple(
                    std::make_pair(std::move(get_block_for_descriptor<Descriptors>()._waitForQueue),
                                   futures::Try<typename stream_information_block<Descriptors>::WaitForResult> {ptr})...);
            }

            auto get_wait_for_resolve_set_all(log_index commitIndex) {
                return std::make_tuple(
                    std::make_pair(get_block_for_descriptor<Descriptors>().get_wait_for_resolve_set(commitIndex),
                                   futures::Try {typename stream_information_block<Descriptors>::WaitForResult {}})...);
            }

            // returns a LogIndex to wait for (if necessary)
            auto check_wait_for() -> std::optional<log_index> {
                if (!_pendingWaitFor) {
                    // we have to trigger a waitFor operation
                    // and wait for the next index
                    _pendingWaitFor = true;
                    return _firstUncommittedIndex;
                }
                return std::nullopt;
            }

            auto min_release_index() -> log_index {
                return std::min({get_block_for_descriptor<Descriptors>()._releaseIndex...});
            }

            template<typename Descriptor>
            auto get_block_for_descriptor() -> stream_information_block<Descriptor> & {
                return std::get<stream_information_block<Descriptor>>(_blocks);
            }
        };

        auto shared_from_self() -> std::shared_ptr<Derived> {
            return std::static_pointer_cast<Derived>(static_cast<Derived &>(*this).shared_from_this());
        }
        auto weak_from_self() -> std::weak_ptr<Derived> {
            return shared_from_self();
        }

        Guarded<multiplexer_data<Spec>, basics::UnshackledMutex> _guardedData {};
    };

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// '<class>': inherits '<method>' via dominance
#pragma warning(disable : 4250)
#endif

    template<typename Spec>
    struct log_demultiplexer_implementation
        : log_demultiplexer<Spec>,    // implement the actual class
                                            ProxyStreamDispatcher<log_demultiplexer_implementation<Spec>, Spec,
                                                                  Stream>,    // use a proxy stream dispatcher
          log_multiplexer_implementation_base<log_demultiplexer_implementation<Spec>, Spec,
                                                                             nil::dbms::replication_sdk::streams::Stream> {
        explicit log_demultiplexer_implementation(std::shared_ptr<replicated_log::ilog_participant> interface_) :
            _interface(std::move(interface_)) {
        }

        auto digestIterator(LogIterator &iter) -> void override {
            this->_guardedData.getLockedGuard()->digest_iterator(iter);
        }

        auto listen() -> void override {
            auto nextIndex = this->_guardedData.doUnderLock([](auto &self) -> std::optional<log_index> {
                if (!self._pendingWaitFor) {
                    self._pendingWaitFor = true;
                    return self._firstUncommittedIndex;
                }
                return std::nullopt;
            });
            if (nextIndex.has_value()) {
                triggerWaitFor(*nextIndex);
            }
        }

        std::shared_ptr<replicated_log::ilog_participant> _interface;

    private:
        void triggerWaitFor(log_index waitForIndex) {
            this->_interface->waitForIterator(waitForIndex)
                .thenFinal(
                    [weak = this->weak_from_this()](futures::Try<std::unique_ptr<LogRangeIterator>> &&result) noexcept {
                        if (auto locked = weak.lock(); locked) {
                            auto that = std::static_pointer_cast<log_demultiplexer_implementation>(locked);
                            try {
                                auto iter = std::move(result).get();    // potentially throws an exception
                                auto [nextIndex, promiseSets] = that->_guardedData.doUnderLock([&](auto &self) {
                                    self._firstUncommittedIndex = iter->range().to;
                                    self.digest_iterator(*iter);
                                    return std::make_tuple(
                                        self._firstUncommittedIndex,
                                                           self.get_wait_for_resolve_set_all(
                                                               self._firstUncommittedIndex.saturated_decrement()));
                                });

                                that->triggerWaitFor(nextIndex);
                                resolve_promise_sets(Spec {}, promiseSets);
                            } catch (replicated_log::participant_resigned_exception const &) {
                                LOG_TOPIC("c5c04", DEBUG, Logger::REPLICATION2)
                                    << "demultiplexer received follower-resigned exception";
                                that->resolve_leader_change(std::current_exception());
                            } catch (basics::Exception const &e) {
                                TRI_ASSERT(e.code() != TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED);
                                LOG_TOPIC("2e28d", FATAL, Logger::REPLICATION2)
                                    << "demultiplexer received unexpected exception: " << e.what();
                                FATAL_ERROR_EXIT();
                            } catch (std::exception const &e) {
                                LOG_TOPIC("2e28d", FATAL, Logger::REPLICATION2)
                                    << "demultiplexer received unexpected exception: " << e.what();
                                FATAL_ERROR_EXIT();
                            } catch (...) {
                                LOG_TOPIC("c3a3d", FATAL, Logger::REPLICATION2)
                                    << "demultiplexer received unexpected exception";
                                FATAL_ERROR_EXIT();
                            }
                        }
                    });
        }
    };

    template<typename Spec>
    struct log_multiplexer_implementation
        : log_multiplexer<Spec>,
          ProxyStreamDispatcher<log_multiplexer_implementation<Spec>, Spec, producer_stream>,
          log_multiplexer_implementation_base<log_multiplexer_implementation<Spec>, Spec,
                                           nil::dbms::replication_sdk::streams::producer_stream> {
        using SelfClass = log_multiplexer_implementation<Spec>;

        explicit log_multiplexer_implementation(std::shared_ptr<replicated_log::ilog_leader> interface_) :
            _interface(std::move(interface_)) {
        }

        void digestAvailableEntries() override {
            auto log = _interface->copyInMemoryLog();
            auto iter = log.get_iterator_from(log_index {0});
            auto waitForIndex = this->_guardedData.doUnderLock([&](auto &self) {
                self.digest_iterator(*iter);
                return self.check_wait_for();
            });
            if (waitForIndex.has_value()) {
                triggerWaitForIndex(*waitForIndex);
            }
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>>
        auto insertInternalDeferred(T const &t) -> std::pair<log_index, deferred_action> {
            auto serialized = std::invoke([&] {
                velocypack::Builder builder;
                multiplexed_values::toVelocyPack<StreamDescriptor>(t, builder);
                return log_payload::create_from_slice(builder.slice());
            });

            // we have to lock before we insert, otherwise we could mess up the order
            // of log entries for this stream
            auto [index, waitForIndex] = this->_guardedData.doUnderLock([&](auto &self) {
                // First write to replicated log - note that insert could trigger a
                // waitFor to be resolved. Therefore, we should hold the lock.
                auto insertIndex =
                    _interface->insert(serialized, false, replicated_log::log_leader::doNotTriggerAsyncReplication);
                TRI_ASSERT(insertIndex > self._lastIndex);
                self._lastIndex = insertIndex;

                // Now we insert the value T into the StreamsLog,
                // but it is not yet visible because of the commitIndex
                auto &block = self.template get_block_for_descriptor<StreamDescriptor>();
                block.append_entry(insertIndex, t);
                return std::make_pair(insertIndex, self.check_wait_for());
            });

            if (waitForIndex.has_value()) {
                triggerWaitForIndex(*waitForIndex);
            }

            // TODO - HACK: because LogLeader::insert can
            // resolve waitFor promises
            // we have a possible deadlock with
            // triggerWaitForIndex callback. This is
            // circumvented by first inserting the entry but
            // not triggering replication immediately. We
            // trigger it here instead.
            // Note that the MSVC STL has a #define interface... macro.
            return std::make_pair(
                index, deferred_action([interface_ = _interface]() noexcept { interface_->triggerAsyncReplication(); }));
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>>
        auto insertInternal(T const &t) -> log_index {
            auto [index, da] = insertInternalDeferred<StreamDescriptor>(t);
            da.fire();
            return index;
        }

        std::shared_ptr<replicated_log::ilog_leader> _interface;

    private:
        void triggerWaitForIndex(log_index waitForIndex) {
            LOG_TOPIC("2b7b1", TRACE, Logger::REPLICATION2) << "multiplexer trigger wait for index " << waitForIndex;
            auto f = this->_interface->waitFor(waitForIndex);
            std::move(f).thenFinal([weak = this->weak_from_this()](
                                       futures::Try<replicated_log::wait_for_result> &&tryResult) noexcept {
                LOG_TOPIC("2b7b1", TRACE, Logger::REPLICATION2) << "multiplexer trigger wait for returned";
                // First lock the shared pointer
                if (auto locked = weak.lock(); locked) {
                    auto that = std::static_pointer_cast<SelfClass>(locked);
                    try {
                        auto &result = tryResult.get();
                        // now acquire the mutex
                        auto [resolveSets, nextIndex] = that->_guardedData.doUnderLock([&](auto &self) {
                            self._pendingWaitFor = false;

                            // find out what the commit index is
                            self._firstUncommittedIndex = result.currentCommitIndex + 1;
                            LOG_TOPIC("2b7b1", TRACE, Logger::REPLICATION2)
                                << "multiplexer update commit index to " << result.currentCommitIndex;
                            return std::make_pair(self.get_wait_for_resolve_set_all(result.currentCommitIndex),
                                                  self.check_wait_for());
                        });

                        resolve_promise_sets(Spec {}, resolveSets);
                        if (nextIndex.has_value()) {
                            that->triggerWaitForIndex(*nextIndex);
                        }
                    } catch (replicated_log::participant_resigned_exception const &) {
                        LOG_TOPIC("c5c05", DEBUG, Logger::REPLICATION2)
                            << "multiplexer received leader-resigned exception";
                        that->resolve_leader_change(std::current_exception());
                    } catch (basics::Exception const &e) {
                        LOG_TOPIC("2e28e", FATAL, Logger::REPLICATION2)
                            << "multiplexer received unexpected exception: " << e.what();
                        FATAL_ERROR_EXIT();
                    } catch (std::exception const &e) {
                        LOG_TOPIC("709f9", FATAL, Logger::REPLICATION2)
                            << "multiplexer received unexpected exception: " << e.what();
                        FATAL_ERROR_EXIT();
                    } catch (...) {
                        LOG_TOPIC("c3a3e", FATAL, Logger::REPLICATION2) << "multiplexer received unexpected exception";
                        FATAL_ERROR_EXIT();
                    }
                }
            });
        }
    };

#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

    template<typename Spec>
    auto log_demultiplexer<Spec>::construct(std::shared_ptr<replicated_log::ilog_participant> interface_)
        -> std::shared_ptr<log_demultiplexer> {
        return std::make_shared<streams::log_demultiplexer_implementation<Spec>>(std::move(interface_));
    }

    template<typename Spec>
    auto
        log_multiplexer<Spec>::construct(std::shared_ptr<nil::dbms::replication_sdk::replicated_log::ilog_leader> leader)
        -> std::shared_ptr<log_multiplexer> {
        return std::make_shared<streams::log_multiplexer_implementation<Spec>>(std::move(leader));
    }

}    // namespace nil::dbms::replication_sdk::streams
