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

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>

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
#include <immer/box.hpp>
#if (_MSC_VER >= 1)
#pragma warning(pop)
#endif

#include <velocypack/Buffer.h>
#include <velocypack/SharedSlice.h>
#include <velocypack/Slice.h>
#include <velocypack/Value.h>

#include "inspection/status.h"

#include <basics/identifier.h>
#include <containers/immer_memory_policy.h>

namespace nil::dbms::velocypack {
    class Builder;
    class Slice;
}    // namespace nil::dbms::velocypack

namespace nil::dbms::replication_sdk {

    struct log_index {
        constexpr log_index() noexcept : value {0} {
        }
        constexpr explicit log_index(std::uint64_t value) noexcept : value {value} {
        }
        std::uint64_t value;

        [[nodiscard]] auto saturated_decrement(uint64_t delta = 1) const noexcept -> log_index;

        bool operator<(const log_index &rhs) const {
            return value < rhs.value;
        }
        bool operator>(const log_index &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const log_index &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const log_index &rhs) const {
            return !(*this < rhs);
        }

        bool operator==(const log_index &rhs) const {
            return value == rhs.value;
        }
        bool operator!=(const log_index &rhs) const {
            return !(rhs == *this);
        }

        [[nodiscard]] auto operator+(std::uint64_t delta) const -> log_index;

        friend auto operator<<(std::ostream &, log_index) -> std::ostream &;

        [[nodiscard]] explicit operator velocypack::Value() const noexcept;
    };

    auto operator<<(std::ostream &, log_index) -> std::ostream &;

    template<class Inspector>
    auto inspect(Inspector &f, log_index &x) {
        if constexpr (Inspector::isLoading) {
            auto v = uint64_t {0};
            auto res = f.apply(v);
            if (res.ok()) {
                x = log_index(v);
            }
            return res;
        } else {
            return f.apply(x.value);
        }
    }

    struct log_term {
        constexpr log_term() noexcept : value {0} {
        }
        constexpr explicit log_term(std::uint64_t value) noexcept : value {value} {
        }
        std::uint64_t value;

        bool operator==(const log_term &rhs) const {
            return value == rhs.value;
        }
        bool operator!=(const log_term &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const log_term &rhs) const {
            return value < rhs.value;
        }
        bool operator>(const log_term &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const log_term &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const log_term &rhs) const {
            return !(*this < rhs);
        }

        friend auto operator<<(std::ostream &, log_term) -> std::ostream &;

        [[nodiscard]] explicit operator velocypack::Value() const noexcept;
    };

    auto operator<<(std::ostream &, log_term) -> std::ostream &;

    template<class Inspector>
    auto inspect(Inspector &f, log_term &x) {
        if constexpr (Inspector::isLoading) {
            auto v = uint64_t {0};
            auto res = f.apply(v);
            if (res.ok()) {
                x = log_term(v);
            }
            return res;
        } else {
            return f.apply(x.value);
        }
    }

    [[nodiscard]] auto to_string(log_term term) -> std::string;
    [[nodiscard]] auto to_string(log_index index) -> std::string;

    struct term_index_pair {
        log_term term {};
        log_index index {};

        term_index_pair(log_term term, log_index index) noexcept;
        term_index_pair() = default;

        void toVelocyPack(velocypack::Builder &builder) const;
        [[nodiscard]] static auto fromVelocyPack(velocypack::Slice) -> term_index_pair;

        bool operator==(const term_index_pair &rhs) const {
            return term == rhs.term && index == rhs.index;
        }
        bool operator!=(const term_index_pair &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const term_index_pair &rhs) const {
            if (term < rhs.term)
                return true;
            if (rhs.term < term)
                return false;
            return index < rhs.index;
        }
        bool operator>(const term_index_pair &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const term_index_pair &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const term_index_pair &rhs) const {
            return !(*this < rhs);
        }

        friend auto operator<<(std::ostream &, term_index_pair) -> std::ostream &;
    };

    auto operator<<(std::ostream &, term_index_pair) -> std::ostream &;

    template<class Inspector>
    auto inspect(Inspector &f, term_index_pair &x) {
        return f.object(x).fields(f.field("term", x.term), f.field("index", x.index));
    }

    struct log_range {
        log_index from;
        log_index to;

        log_range(log_index from, log_index to) noexcept;

        [[nodiscard]] auto empty() const noexcept -> bool;
        [[nodiscard]] auto count() const noexcept -> std::size_t;
        [[nodiscard]] auto contains(log_index idx) const noexcept -> bool;

        friend auto operator<<(std::ostream &os, log_range const &r) -> std::ostream &;
        friend auto intersect(log_range a, log_range b) noexcept -> log_range;

        struct Iterator {
            auto operator++() noexcept -> Iterator &;
            auto operator++(int) noexcept -> Iterator;
            auto operator*() const noexcept -> log_index;
            auto operator->() const noexcept -> log_index const *;

            bool operator==(const Iterator &rhs) const {
                return current == rhs.current;
            }
            bool operator!=(const Iterator &rhs) const {
                return !(rhs == *this);
            }

        private:
            friend log_range;
            explicit Iterator(log_index idx) : current(idx) {
            }
            log_index current;
        };

        bool operator==(const log_range &rhs) const {
            return from == rhs.from && to == rhs.to;
        }
        bool operator!=(const log_range &rhs) const {
            return !(rhs == *this);
        }

        [[nodiscard]] auto begin() const noexcept -> Iterator;
        [[nodiscard]] auto end() const noexcept -> Iterator;
    };

    auto operator<<(std::ostream &os, log_range const &r) -> std::ostream &;

    template<class Inspector>
    auto inspect(Inspector &f, log_range &x) {
        return f.object(x).fields(f.field("from", x.from), f.field("to", x.to));
    }

    auto intersect(log_range a, log_range b) noexcept -> log_range;
    auto to_string(log_range const &) -> std::string;

    using ParticipantId = std::string;

    class LogId : public nil::dbms::basics::Identifier {
    public:
        using nil::dbms::basics::Identifier::Identifier;

        static auto fromString(std::string_view) noexcept -> std::optional<LogId>;

        [[nodiscard]] explicit operator velocypack::Value() const noexcept;
    };

    template<class Inspector>
    auto inspect(Inspector &f, LogId &x) {
        if constexpr (Inspector::isLoading) {
            auto v = uint64_t {0};
            auto res = f.apply(v);
            if (res.ok()) {
                x = LogId(v);
            }
            return res;
        } else {
            // TODO this is a hack to make the compiler happy who does not want
            //      to assign x.id() (unsigned long int) to what it expects (unsigned
            //      long int&)
            auto id = x.id();
            return f.apply(id);
        }
    }

    auto to_string(LogId logId) -> std::string;

    struct global_log_identifier {
        global_log_identifier(const std::string &database, LogId id);
        std::string database;
        LogId id;
    };

    auto to_string(global_log_identifier const &) -> std::string;

    struct participant_flags {
        bool forced = false;
        bool allowedInQuorum = true;
        bool allowedAsLeader = true;

        bool operator<(const participant_flags &rhs) const {
            if (forced < rhs.forced)
                return true;
            if (rhs.forced < forced)
                return false;
            if (allowedInQuorum < rhs.allowedInQuorum)
                return true;
            if (rhs.allowedInQuorum < allowedInQuorum)
                return false;
            return allowedAsLeader < rhs.allowedAsLeader;
        }
        bool operator>(const participant_flags &rhs) const {
            return rhs < *this;
        }
        bool operator<=(const participant_flags &rhs) const {
            return !(rhs < *this);
        }
        bool operator>=(const participant_flags &rhs) const {
            return !(*this < rhs);
        }

        bool operator==(const participant_flags &rhs) const {
            return forced == rhs.forced && allowedInQuorum == rhs.allowedInQuorum &&
                   allowedAsLeader == rhs.allowedAsLeader;
        }
        bool operator!=(const participant_flags &rhs) const {
            return !(rhs == *this);
        }

        friend auto operator<<(std::ostream &, participant_flags const &) -> std::ostream &;

        void toVelocyPack(velocypack::Builder &) const;
        static auto fromVelocyPack(velocypack::Slice) -> participant_flags;
    };

    template<class Inspector>
    auto inspect(Inspector &f, participant_flags &x) {
        return f.object(x).fields(f.field("forced", x.forced).fallback(false),
                                  f.field("allowedInQuorum", x.allowedInQuorum).fallback(true),
                                  f.field("allowedAsLeader", x.allowedAsLeader).fallback(true));
    }

    auto operator<<(std::ostream &, participant_flags const &) -> std::ostream &;

    // These settings are initialised by the ReplicatedLogFeature based on command
    // line arguments
    struct replicated_log_global_settings {
    public:
        static inline constexpr std::size_t defaultThresholdNetworkBatchSize {1024 * 1024};
        static inline constexpr std::size_t minThresholdNetworkBatchSize {1024 * 1024};

        static inline constexpr std::size_t defaultThresholdRocksDBWriteBatchSize {1024 * 1024};
        static inline constexpr std::size_t minThresholdRocksDBWriteBatchSize {1024 * 1024};

        std::size_t _thresholdNetworkBatchSize {defaultThresholdNetworkBatchSize};
        std::size_t _thresholdRocksDBWriteBatchSize {defaultThresholdRocksDBWriteBatchSize};
    };

    namespace replicated_log {
        /*
         * Indicates why the commit index is not increasing as expected.
         * Even though some pending entries might have been committed, unless all
         * pending entries are committed, we say the commit index is behind. This object
         * gives an indication of why might that be.
         */
        struct commit_fail_reason {
            commit_fail_reason() = default;

            struct nothing_to_commit {
                static auto fromVelocyPack(velocypack::Slice) -> nothing_to_commit;
                void toVelocyPack(velocypack::Builder &builder) const;

                bool operator==(const nothing_to_commit &rhs) const {
                    return true;
                }
            };
            struct quorum_size_not_reached {
                struct participant_info {
                    bool isFailed {};
                    bool isAllowedInQuorum {};
                    term_index_pair lastAcknowledged;
                    static auto fromVelocyPack(velocypack::Slice) -> participant_info;
                    void toVelocyPack(velocypack::Builder &builder) const;

                    bool operator==(const participant_info &rhs) const {
                        return isFailed == rhs.isFailed && isAllowedInQuorum == rhs.isAllowedInQuorum &&
                               lastAcknowledged == rhs.lastAcknowledged;
                    }
                    bool operator!=(const participant_info &rhs) const {
                        return !(rhs == *this);
                    }
                };
                using who_type = std::unordered_map<ParticipantId, participant_info>;
                static auto fromVelocyPack(velocypack::Slice) -> quorum_size_not_reached;
                void toVelocyPack(velocypack::Builder &builder) const;
                who_type who;
                term_index_pair spearhead;

                bool operator==(const quorum_size_not_reached &rhs) const {
                    return who == rhs.who && spearhead == rhs.spearhead;
                }
                bool operator!=(const quorum_size_not_reached &rhs) const {
                    return !(rhs == *this);
                }
            };
            struct forced_participant_not_in_quorum {
                static auto fromVelocyPack(velocypack::Slice) -> forced_participant_not_in_quorum;
                void toVelocyPack(velocypack::Builder &builder) const;
                ParticipantId who;

                bool operator==(const forced_participant_not_in_quorum &rhs) const {
                    return who == rhs.who;
                }
                bool operator!=(const forced_participant_not_in_quorum &rhs) const {
                    return !(rhs == *this);
                }
            };
            struct non_eligible_server_required_for_quorum {
                enum Why {
                    kNotAllowedInQuorum,
                    // WrongTerm might be misleading, because the follower might be in the
                    // right term, it just never has acked an entry of the current term.
                    kWrongTerm,
                };
                static auto to_string(Why) noexcept -> std::string_view;

                using CandidateMap = std::unordered_map<ParticipantId, Why>;

                CandidateMap candidates;

                static auto fromVelocyPack(velocypack::Slice) -> non_eligible_server_required_for_quorum;
                void toVelocyPack(velocypack::Builder &builder) const;

                bool operator==(const non_eligible_server_required_for_quorum &rhs) const {
                    return candidates == rhs.candidates;
                }
                bool operator!=(const non_eligible_server_required_for_quorum &rhs) const {
                    return !(rhs == *this);
                }
            };
            struct fewer_participants_than_write_concern {
                std::size_t effectiveWriteConcern {};
                std::size_t numParticipants {};
                static auto fromVelocyPack(velocypack::Slice) -> fewer_participants_than_write_concern;
                void toVelocyPack(velocypack::Builder &builder) const;

                bool operator==(const fewer_participants_than_write_concern &rhs) const {
                    return effectiveWriteConcern == rhs.effectiveWriteConcern && numParticipants == rhs.numParticipants;
                }
                bool operator!=(const fewer_participants_than_write_concern &rhs) const {
                    return !(rhs == *this);
                }
            };
            std::variant<nothing_to_commit,
                         quorum_size_not_reached,
                         forced_participant_not_in_quorum,
                         non_eligible_server_required_for_quorum,
                         fewer_participants_than_write_concern>
                value;

            static auto with_nothing_to_commit() noexcept -> commit_fail_reason;
            static auto with_quorum_size_not_reached(quorum_size_not_reached::who_type who, term_index_pair spearhead) noexcept
                -> commit_fail_reason;
            static auto with_forced_participant_not_in_quorum(ParticipantId who) noexcept -> commit_fail_reason;
            static auto
                withNonEligibleServerRequiredForQuorum(non_eligible_server_required_for_quorum::CandidateMap) noexcept
                -> commit_fail_reason;
            // This would have too many `std::size_t` arguments to not be confusing,
            // so taking the full object instead.
            static auto withFewerParticipantsThanWriteConcern(fewer_participants_than_write_concern) -> commit_fail_reason;

            static auto fromVelocyPack(velocypack::Slice) -> commit_fail_reason;
            void toVelocyPack(velocypack::Builder &builder) const;

            bool operator==(const commit_fail_reason &rhs) const {
                return value == rhs.value;
            }
            bool operator!=(const commit_fail_reason &rhs) const {
                return !(rhs == *this);
            }

        private:
            template<typename... Args>
            explicit commit_fail_reason(std::in_place_t, Args &&...args) noexcept;
        };

        template<class Inspector>
        auto inspect(Inspector &f, nil::dbms::replication_sdk::replicated_log::commit_fail_reason &x) {
            if constexpr (Inspector::isLoading) {
                x = commit_fail_reason::fromVelocyPack(f.slice());
            } else {
                x.toVelocyPack(f.builder());
            }
            return nil::dbms::inspection::Status::Success {};
        }

        auto operator<<(std::ostream &, commit_fail_reason::quorum_size_not_reached::participant_info) -> std::ostream &;

        auto to_string(commit_fail_reason const &) -> std::string;
    }    // namespace replicated_log

}    // namespace nil::dbms::replication_sdk

namespace nil::dbms {
    template<>
    struct velocypack::Extractor<replication_sdk::log_term> {
        static auto extract(velocypack::Slice slice) -> replication_sdk::log_term {
            return replication_sdk::log_term {slice.getNumericValue<std::uint64_t>()};
        }
    };

    template<>
    struct velocypack::Extractor<replication_sdk::log_index> {
        static auto extract(velocypack::Slice slice) -> replication_sdk::log_index {
            return replication_sdk::log_index {slice.getNumericValue<std::uint64_t>()};
        }
    };

    template<>
    struct velocypack::Extractor<replication_sdk::LogId> {
        static auto extract(velocypack::Slice slice) -> replication_sdk::LogId {
            return replication_sdk::LogId {slice.getNumericValue<std::uint64_t>()};
        }
    };

}    // namespace nil::dbms

template<>
struct fmt::formatter<nil::dbms::replication_sdk::LogId> : fmt::formatter<nil::dbms::basics::Identifier> { };

template<>
struct std::hash<nil::dbms::replication_sdk::log_index> {
    [[nodiscard]] auto operator()(nil::dbms::replication_sdk::log_index const &v) const noexcept -> std::size_t {
        return std::hash<uint64_t> {}(v.value);
    }
};

template<>
struct std::hash<nil::dbms::replication_sdk::log_term> {
    [[nodiscard]] auto operator()(nil::dbms::replication_sdk::log_term const &v) const noexcept -> std::size_t {
        return std::hash<uint64_t> {}(v.value);
    }
};

template<>
struct std::hash<nil::dbms::replication_sdk::LogId> {
    [[nodiscard]] auto operator()(nil::dbms::replication_sdk::LogId const &v) const noexcept -> std::size_t {
        return std::hash<nil::dbms::basics::Identifier> {}(v);
    }
};
