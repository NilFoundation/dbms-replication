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
#include <nil/dbms/replication/exceptions/participant_resigned_exception.hpp>

#include <nil/dbms/replication/log/ilog_interfaces.hpp>

#include <nil/dbms/replication/streams/multiplexed_values.hpp>
#include <nil/dbms/replication/streams/stream_information_block.hpp>
#include "streams.hpp"
#include <nil/dbms/replication/streams/stream_information_block.tpp>
#include <nil/dbms/replication/streams/streams.tpp>

namespace nil::dbms::replication::streams {

    namespace {
        template<typename Queue, typename Result>
        auto allUnresolved(std::pair<Queue, Result> &q) {
            return std::all_of(std::begin(q.first), std::end(q.first),
                               [&](auto const &pair) { return !pair.second.isFulfilled(); });
        }
        template<typename Descriptor, typename Queue, typename Result,
                 typename Block = StreamInformationBlock<Descriptor>>
        auto resolvePromiseSet(std::pair<Queue, Result> &q) {
            TRI_ASSERT(allUnresolved(q));
            std::for_each(std::begin(q.first), std::end(q.first), [&](auto &pair) {
                TRI_ASSERT(!pair.second.isFulfilled());
                if (!pair.second.isFulfilled()) {
                    pair.second.setTry(std::move(q.second));
                }
            });
        }

        template<typename... Descriptors, typename... Pairs, std::size_t... Idxs>
        auto resolvePromiseSets(stream_descriptor_set<Descriptors...>,
                                std::index_sequence<Idxs...>,
                                std::tuple<Pairs...> &pairs) {
            (resolvePromiseSet<Descriptors>(std::get<Idxs>(pairs)), ...);
        }

        template<typename... Descriptors, typename... Pairs>
        auto resolvePromiseSets(stream_descriptor_set<Descriptors...>, std::tuple<Pairs...> &pairs) {
            resolvePromiseSets(stream_descriptor_set<Descriptors...> {}, std::index_sequence_for<Descriptors...> {},
                               pairs);
        }
    }    // namespace

    template<typename Derived, typename Spec, template<typename> typename StreamInterface>
    struct LogMultiplexerImplementationBase {
        explicit LogMultiplexerImplementationBase() : _guarded_data(static_cast<Derived &>(*this)) {
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>,
                 typename E = StreamEntryView<T>>
        auto waitForIteratorInternal(log_index first) -> futures::Future<std::unique_ptr<Typedlog_rangeIterator<E>>> {
            return waitForInternal<StreamDescriptor>(first).thenValue(
                [weak = weak_from_self(), first](auto &&) -> std::unique_ptr<Typedlog_rangeIterator<E>> {
                    if (auto that = weak.lock(); that != nullptr) {
                        return that->_guarded_data.doUnderLock([&](MultiplexerData<Spec> &self) {
                            auto &block = std::get<StreamInformationBlock<StreamDescriptor>>(self._blocks);
                            return block.getIteratorRange(first, self._firstUncommittedIndex);
                        });
                    } else {
                        return nullptr;
                    }
                });
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>,
                 typename W = typename Stream<T>::wait_for_result>
        auto waitForInternal(log_index index) -> futures::Future<W> {
            return _guarded_data.doUnderLock([&](MultiplexerData<Spec> &self) {
                if (self._firstUncommittedIndex > index) {
                    return futures::Future<W> {std::in_place};
                }
                auto &block = std::get<StreamInformationBlock<StreamDescriptor>>(self._blocks);
                return block.registerWaitFor(index);
            });
        }

        template<typename StreamDescriptor>
        auto releaseInternal(log_index index) -> void {
            // update the release index for the given stream
            // then compute the minimum and forward it to the
            // actual log implementation
            auto globalReleaseIndex =
                _guarded_data.doUnderLock([&](MultiplexerData<Spec> &self) -> std::optional<log_index> {
                    {
                        auto &block = self.template getBlockForDescriptor<StreamDescriptor>();
                        auto newIndex = std::max(block._releaseIndex, index);
                        if (newIndex == block._releaseIndex) {
                            return std::nullopt;
                        }
                        TRI_ASSERT(newIndex > block._releaseIndex);
                        block._releaseIndex = newIndex;
                    }

                    return self.minReleaseIndex();
                });

            if (globalReleaseIndex) {
                // TODO handle return value
                std::ignore = getLogInterface()->release(*globalReleaseIndex);
            }
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>,
                 typename E = StreamEntryView<T>>
        auto getIteratorInternal() -> std::unique_ptr<Typedlog_rangeIterator<E>> {
            return _guarded_data.doUnderLock([](MultiplexerData<Spec> &self) {
                auto &block = self.template getBlockForDescriptor<StreamDescriptor>();
                return block.getIterator();
            });
        }

        auto resolveLeaderChange(std::exception_ptr ptr) {
            auto promiseSet = _guarded_data.getLockedGuard()->getChangeLeaderResolveSet(ptr);
            resolvePromiseSets(Spec {}, promiseSet);
        }

    protected:
        auto &getLogInterface() {
            return static_cast<Derived &>(*this)._interface;
        }

        template<typename>
        struct MultiplexerData;
        template<typename... Descriptors>
        struct MultiplexerData<stream_descriptor_set<Descriptors...>> {
            std::tuple<StreamInformationBlock<Descriptors>...> _blocks;
            log_index _firstUncommittedIndex {1};
            log_index _lastIndex;
            bool _pendingWaitFor {false};

            Derived &_self;

            explicit MultiplexerData(Derived &self) : _self(self) {
            }
            void digestIterator(LogIterator &iter) {
                while (auto memtry = iter.next()) {
                    auto muxedValue = MultiplexedValues::from_velocy_pack<Descriptors...>(memtry->log_payload());
                    std::visit(
                        [&](auto &&value) {
                            using ValueTag = std::decay_t<decltype(value)>;
                            using Descriptor = typename ValueTag::DescriptorType;
                            std::get<StreamInformationBlock<Descriptor>>(_blocks).appendEntry(memtry->log_index(),
                                                                                              std::move(value.value));
                        },
                        std::move(muxedValue.variant()));
                }
            }

            auto getChangeLeaderResolveSet(std::exception_ptr ptr) {
                return std::make_tuple(
                    std::make_pair(std::move(getBlockForDescriptor<Descriptors>()._waitForQueue),
                                   futures::Try<typename StreamInformationBlock<Descriptors>::wait_for_result> {ptr})...);
            }

            auto getWaitForResolveSetAll(log_index commitIndex) {
                return std::make_tuple(
                    std::make_pair(getBlockForDescriptor<Descriptors>().getWaitForResolveSet(commitIndex),
                                   futures::Try {typename StreamInformationBlock<Descriptors>::wait_for_result {}})...);
            }

            // returns a log_index to wait for (if necessary)
            auto checkWaitFor() -> std::optional<log_index> {
                if (!_pendingWaitFor) {
                    // we have to trigger a waitFor operation
                    // and wait for the next index
                    _pendingWaitFor = true;
                    return _firstUncommittedIndex;
                }
                return std::nullopt;
            }

            auto minReleaseIndex() -> log_index {
                return std::min({getBlockForDescriptor<Descriptors>()._releaseIndex...});
            }

            template<typename Descriptor>
            auto getBlockForDescriptor() -> StreamInformationBlock<Descriptor> & {
                return std::get<StreamInformationBlock<Descriptor>>(_blocks);
            }
        };

        auto shared_from_self() -> std::shared_ptr<Derived> {
            return std::static_pointer_cast<Derived>(static_cast<Derived &>(*this).shared_from_this());
        }
        auto weak_from_self() -> std::weak_ptr<Derived> {
            return shared_from_self();
        }

        Guarded<MultiplexerData<Spec>, basics::UnshackledMutex> _guarded_data {};
    };

#if (_MSC_VER >= 1)
// suppress warnings:
#pragma warning(push)
// '<class>': inherits '<method>' via dominance
#pragma warning(disable : 4250)
#endif

    template<typename Spec>
    struct LogDemultiplexerImplementation : LogDemultiplexer<Spec>,    // implement the actual class
                                            ProxyStreamDispatcher<LogDemultiplexerImplementation<Spec>, Spec,
                                                                  Stream>,    // use a proxy stream dispatcher
                                            LogMultiplexerImplementationBase<LogDemultiplexerImplementation<Spec>, Spec,
                                                                             nil::dbms::replication::streams::Stream> {
        explicit LogDemultiplexerImplementation(std::shared_ptr<log::ILogParticipant> interface_) :
            _interface(std::move(interface_)) {
        }

        auto digestIterator(LogIterator &iter) -> void override {
            this->_guarded_data.getLockedGuard()->digestIterator(iter);
        }

        auto listen() -> void override {
            auto nextIndex = this->_guarded_data.doUnderLock([](auto &self) -> std::optional<log_index> {
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

        std::shared_ptr<log::ILogParticipant> _interface;

    private:
        void triggerWaitFor(log_index waitForIndex) {
            this->_interface->waitForIterator(waitForIndex)
                .thenFinal(
                    [weak = this->weak_from_this()](futures::Try<std::unique_ptr<log_rangeIterator>> &&result) noexcept {
                        if (auto locked = weak.lock(); locked) {
                            auto that = std::static_pointer_cast<LogDemultiplexerImplementation>(locked);
                            try {
                                auto iter = std::move(result).get();    // potentially throws an exception
                                auto [nextIndex, promiseSets] = that->_guarded_data.doUnderLock([&](auto &self) {
                                    self._firstUncommittedIndex = iter->range().to;
                                    self.digestIterator(*iter);
                                    return std::make_tuple(
                                        self._firstUncommittedIndex,
                                        self.getWaitForResolveSetAll(self._firstUncommittedIndex.saturatedDecrement()));
                                });

                                that->triggerWaitFor(nextIndex);
                                resolvePromiseSets(Spec {}, promiseSets);
                            } catch (log::participant_resigned_exception const &) {
                                LOG_TOPIC("c5c04", DEBUG, Logger::replication)
                                    << "demultiplexer received follower-resigned exception";
                                that->resolveLeaderChange(std::current_exception());
                            } catch (basics::Exception const &e) {
                                TRI_ASSERT(e.code() != TRI_ERROR_REPLICATION_REPLICATED_LOG_LEADER_RESIGNED);
                                LOG_TOPIC("2e28d", FATAL, Logger::replication)
                                    << "demultiplexer received unexpected exception: " << e.what();
                                FATAL_ERROR_EXIT();
                            } catch (std::exception const &e) {
                                LOG_TOPIC("2e28d", FATAL, Logger::replication)
                                    << "demultiplexer received unexpected exception: " << e.what();
                                FATAL_ERROR_EXIT();
                            } catch (...) {
                                LOG_TOPIC("c3a3d", FATAL, Logger::replication)
                                    << "demultiplexer received unexpected exception";
                                FATAL_ERROR_EXIT();
                            }
                        }
                    });
        }
    };

    template<typename Spec>
    struct LogMultiplexerImplementation
        : LogMultiplexer<Spec>,
          ProxyStreamDispatcher<LogMultiplexerImplementation<Spec>, Spec, ProducerStream>,
          LogMultiplexerImplementationBase<LogMultiplexerImplementation<Spec>, Spec,
                                           nil::dbms::replication::streams::ProducerStream> {
        using SelfClass = LogMultiplexerImplementation<Spec>;

        explicit LogMultiplexerImplementation(std::shared_ptr<log::Ilog_leader> interface_) :
            _interface(std::move(interface_)) {
        }

        void digestAvailableEntries() override {
            auto log = _interface->copyInMemoryLog();
            auto iter = log.getIteratorFrom(log_index {0});
            auto waitForIndex = this->_guarded_data.doUnderLock([&](auto &self) {
                self.digestIterator(*iter);
                return self.checkWaitFor();
            });
            if (waitForIndex.has_value()) {
                triggerWaitForIndex(*waitForIndex);
            }
        }

        template<typename StreamDescriptor, typename T = stream_descriptor_type_t<StreamDescriptor>>
        auto insertInternalDeferred(T const &t) -> std::pair<log_index, deferred_action> {
            auto serialized = std::invoke([&] {
                velocypack::Builder builder;
                MultiplexedValues::to_velocy_pack<StreamDescriptor>(t, builder);
                return log_payload::create_from_slice(builder.slice());
            });

            // we have to lock before we insert, otherwise we could mess up the order
            // of log entries for this stream
            auto [index, waitForIndex] = this->_guarded_data.doUnderLock([&](auto &self) {
                // First write to replicated log - note that insert could trigger a
                // waitFor to be resolved. Therefore, we should hold the lock.
                auto insertIndex =
                    _interface->insert(serialized, false, log::log_leader::doNotTriggerAsyncReplication);
                TRI_ASSERT(insertIndex > self._lastIndex);
                self._lastIndex = insertIndex;

                // Now we insert the value T into the StreamsLog,
                // but it is not yet visible because of the commitIndex
                auto &block = self.template getBlockForDescriptor<StreamDescriptor>();
                block.appendEntry(insertIndex, t);
                return std::make_pair(insertIndex, self.checkWaitFor());
            });

            if (waitForIndex.has_value()) {
                triggerWaitForIndex(*waitForIndex);
            }

            // TODO - HACK: because log_leader::insert can
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

        std::shared_ptr<log::Ilog_leader> _interface;

    private:
        void triggerWaitForIndex(log_index waitForIndex) {
            LOG_TOPIC("2b7b1", TRACE, Logger::replication) << "multiplexer trigger wait for index " << waitForIndex;
            auto f = this->_interface->waitFor(waitForIndex);
            std::move(f).thenFinal([weak = this->weak_from_this()](
                                       futures::Try<log::wait_for_result> &&tryResult) noexcept {
                LOG_TOPIC("2b7b1", TRACE, Logger::replication) << "multiplexer trigger wait for returned";
                // First lock the shared pointer
                if (auto locked = weak.lock(); locked) {
                    auto that = std::static_pointer_cast<SelfClass>(locked);
                    try {
                        auto &result = tryResult.get();
                        // now acquire the mutex
                        auto [resolveSets, nextIndex] = that->_guarded_data.doUnderLock([&](auto &self) {
                            self._pendingWaitFor = false;

                            // find out what the commit index is
                            self._firstUncommittedIndex = result.currentCommitIndex + 1;
                            LOG_TOPIC("2b7b1", TRACE, Logger::replication)
                                << "multiplexer update commit index to " << result.currentCommitIndex;
                            return std::make_pair(self.getWaitForResolveSetAll(result.currentCommitIndex),
                                                  self.checkWaitFor());
                        });

                        resolvePromiseSets(Spec {}, resolveSets);
                        if (nextIndex.has_value()) {
                            that->triggerWaitForIndex(*nextIndex);
                        }
                    } catch (log::participant_resigned_exception const &) {
                        LOG_TOPIC("c5c05", DEBUG, Logger::replication)
                            << "multiplexer received leader-resigned exception";
                        that->resolveLeaderChange(std::current_exception());
                    } catch (basics::Exception const &e) {
                        LOG_TOPIC("2e28e", FATAL, Logger::replication)
                            << "multiplexer received unexpected exception: " << e.what();
                        FATAL_ERROR_EXIT();
                    } catch (std::exception const &e) {
                        LOG_TOPIC("709f9", FATAL, Logger::replication)
                            << "multiplexer received unexpected exception: " << e.what();
                        FATAL_ERROR_EXIT();
                    } catch (...) {
                        LOG_TOPIC("c3a3e", FATAL, Logger::replication) << "multiplexer received unexpected exception";
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
    auto LogDemultiplexer<Spec>::construct(std::shared_ptr<log::ILogParticipant> interface_)
        -> std::shared_ptr<LogDemultiplexer> {
        return std::make_shared<streams::LogDemultiplexerImplementation<Spec>>(std::move(interface_));
    }

    template<typename Spec>
    auto LogMultiplexer<Spec>::construct(std::shared_ptr<nil::dbms::replication::log::Ilog_leader> leader)
        -> std::shared_ptr<LogMultiplexer> {
        return std::make_shared<streams::LogMultiplexerImplementation<Spec>>(std::move(leader));
    }

}    // namespace nil::dbms::replication::streams
