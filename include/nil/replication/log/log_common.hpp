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

namespace nil {
    namespace dbms {
        namespace velocypack {
            class Builder;

            class Slice;
        }
    }
}    // namespace nil::dbms::velocypack

namespace nil {
    namespace dbms {
        namespace replication {

            struct log_index {
                constexpr log_index()

                noexcept : value{0} {
                }

                constexpr explicit log_index(std::uint64_t value)

                noexcept : value{value} {
                }
                std::uint64_t value;

                [[nodiscard]] auto saturatedDecrement(uint64_t delta = 1) const noexcept -> log_index;

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

                [[nodiscard]] explicit operator velocypack::Value() const

                noexcept;
            };

            auto operator<<(std::ostream &, log_index) -> std::ostream &;

            template<class Inspector>
            auto inspect(Inspector &f, log_index &x) {
                if constexpr(Inspector::isLoading)
                {
                    auto v = uint64_t{0};
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
                constexpr log_term()

                noexcept : value{0} {
                }

                constexpr explicit log_term(std::uint64_t value)

                noexcept : value{value} {
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

                [[nodiscard]] explicit operator velocypack::Value() const

                noexcept;
            };

            auto operator<<(std::ostream &, log_term) -> std::ostream &;

            template<class Inspector>
            auto inspect(Inspector &f, log_term &x) {
                if constexpr(Inspector::isLoading)
                {
                    auto v = uint64_t{0};
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
                log_term term{};
                log_index index{};

                term_index_pair(log_term term, log_index index)

                noexcept;

                term_index_pair() = default;

                void to_velocy_pack(velocypack::Builder &builder) const;

                [[nodiscard]] static auto from_velocy_pack(velocypack::Slice) -> term_index_pair;

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

            class log_id : public nil::dbms::basics::Identifier {
            public:
                using nil::dbms::basics::Identifier::Identifier;

                static auto fromString(std::string_view) noexcept -> std::optional<log_id>;

                [[nodiscard]] explicit operator velocypack::Value() const noexcept;
            };

            template<class Inspector>
            auto inspect(Inspector &f, log_id &x) {
                if constexpr(Inspector::isLoading)
                {
                    auto v = uint64_t{0};
                    auto res = f.apply(v);
                    if (res.ok()) {
                        x = log_id(v);
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

            auto to_string(log_id log_id) -> std::string;

            struct global_log_identifier {
                global_log_identifier(const std::string &database, log_id id);

                std::string database;
                log_id id;
            };

            auto to_string(global_log_identifier const &) -> std::string;

            struct ParticipantFlags {
                bool forced = false;
                bool allowedInQuorum = true;
                bool allowedAsLeader = true;

                bool operator<(const ParticipantFlags &rhs) const {
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

                bool operator>(const ParticipantFlags &rhs) const {
                    return rhs < *this;
                }

                bool operator<=(const ParticipantFlags &rhs) const {
                    return !(rhs < *this);
                }

                bool operator>=(const ParticipantFlags &rhs) const {
                    return !(*this < rhs);
                }

                bool operator==(const ParticipantFlags &rhs) const {
                    return forced == rhs.forced && allowedInQuorum == rhs.allowedInQuorum &&
                           allowedAsLeader == rhs.allowedAsLeader;
                }

                bool operator!=(const ParticipantFlags &rhs) const {
                    return !(rhs == *this);
                }

                friend auto operator<<(std::ostream &, ParticipantFlags const &) -> std::ostream &;

                void to_velocy_pack(velocypack::Builder &) const;

                static auto from_velocy_pack(velocypack::Slice) -> ParticipantFlags;
            };

            template<class Inspector>
            auto inspect(Inspector &f, ParticipantFlags &x) {
                return f.object(x).fields(f.field("forced", x.forced).fallback(false),
                                          f.field("allowedInQuorum", x.allowedInQuorum).fallback(true),
                                          f.field("allowedAsLeader", x.allowedAsLeader).fallback(true));
            }

            auto operator<<(std::ostream &, ParticipantFlags const &) -> std::ostream &;

            // These settings are initialised by the log_feature based on command
            // line arguments
            struct ReplicatedLogGlobalSettings {
            public:
                static inline constexpr std::size_t
                defaultThresholdNetworkBatchSize { 1024 * 1024 };
                static inline constexpr std::size_t
                minThresholdNetworkBatchSize { 1024 * 1024 };

                static inline constexpr std::size_t
                defaultThresholdRocksDBWriteBatchSize { 1024 * 1024 };
                static inline constexpr std::size_t
                minThresholdRocksDBWriteBatchSize { 1024 * 1024 };

                std::size_t _thresholdNetworkBatchSize{defaultThresholdNetworkBatchSize};
                std::size_t _thresholdRocksDBWriteBatchSize{defaultThresholdRocksDBWriteBatchSize};
            };

            namespace log {
                /*
                 * Indicates why the commit index is not increasing as expected.
                 * Even though some pending entries might have been committed, unless all
                 * pending entries are committed, we say the commit index is behind. This object
                 * gives an indication of why might that be.
                 */
                struct CommitFailReason {
                    CommitFailReason() = default;

                    struct NothingToCommit {
                        static auto from_velocy_pack(velocypack::Slice) -> NothingToCommit;

                        void to_velocy_pack(velocypack::Builder &builder) const;

                        bool operator==(const NothingToCommit &rhs) const {
                            return true;
                        }
                    };

                    struct QuorumSizeNotReached {
                        struct ParticipantInfo {
                            bool isFailed{};
                            bool isAllowedInQuorum{};
                            term_index_pair lastAcknowledged;

                            static auto from_velocy_pack(velocypack::Slice) -> ParticipantInfo;

                            void to_velocy_pack(velocypack::Builder &builder) const;

                            bool operator==(const ParticipantInfo &rhs) const {
                                return isFailed == rhs.isFailed && isAllowedInQuorum == rhs.isAllowedInQuorum &&
                                       lastAcknowledged == rhs.lastAcknowledged;
                            }

                            bool operator!=(const ParticipantInfo &rhs) const {
                                return !(rhs == *this);
                            }
                        };

                        using who_type = std::unordered_map<ParticipantId, ParticipantInfo>;

                        static auto from_velocy_pack(velocypack::Slice) -> QuorumSizeNotReached;

                        void to_velocy_pack(velocypack::Builder &builder) const;

                        who_type who;
                        term_index_pair spearhead;

                        bool operator==(const QuorumSizeNotReached &rhs) const {
                            return who == rhs.who && spearhead == rhs.spearhead;
                        }

                        bool operator!=(const QuorumSizeNotReached &rhs) const {
                            return !(rhs == *this);
                        }
                    };

                    struct ForcedParticipantNotInQuorum {
                        static auto from_velocy_pack(velocypack::Slice) -> ForcedParticipantNotInQuorum;

                        void to_velocy_pack(velocypack::Builder &builder) const;

                        ParticipantId who;

                        bool operator==(const ForcedParticipantNotInQuorum &rhs) const {
                            return who == rhs.who;
                        }

                        bool operator!=(const ForcedParticipantNotInQuorum &rhs) const {
                            return !(rhs == *this);
                        }
                    };

                    struct NonEligibleServerRequiredForQuorum {
                        enum Why {
                            kNotAllowedInQuorum,
                            // WrongTerm might be misleading, because the follower might be in the
                            // right term, it just never has acked an entry of the current term.
                            kWrongTerm,
                        };

                        static auto to_string(Why)

                        noexcept ->
                        std::string_view;

                        using CandidateMap = std::unordered_map<ParticipantId, Why>;

                        CandidateMap candidates;

                        static auto from_velocy_pack(velocypack::Slice) -> NonEligibleServerRequiredForQuorum;

                        void to_velocy_pack(velocypack::Builder &builder) const;

                        bool operator==(const NonEligibleServerRequiredForQuorum &rhs) const {
                            return candidates == rhs.candidates;
                        }

                        bool operator!=(const NonEligibleServerRequiredForQuorum &rhs) const {
                            return !(rhs == *this);
                        }
                    };

                    struct FewerParticipantsThanWriteConcern {
                        std::size_t effectiveWriteConcern{};
                        std::size_t numParticipants{};

                        static auto from_velocy_pack(velocypack::Slice) -> FewerParticipantsThanWriteConcern;

                        void to_velocy_pack(velocypack::Builder &builder) const;

                        bool operator==(const FewerParticipantsThanWriteConcern &rhs) const {
                            return effectiveWriteConcern == rhs.effectiveWriteConcern &&
                                   numParticipants == rhs.numParticipants;
                        }

                        bool operator!=(const FewerParticipantsThanWriteConcern &rhs) const {
                            return !(rhs == *this);
                        }
                    };

                    std::variant <NothingToCommit,
                    QuorumSizeNotReached,
                    ForcedParticipantNotInQuorum,
                    NonEligibleServerRequiredForQuorum,
                    FewerParticipantsThanWriteConcern>
                            value;

                    static auto withNothingToCommit() noexcept -> CommitFailReason;

                    static auto withQuorumSizeNotReached(QuorumSizeNotReached::who_type who, term_index_pair spearhead) noexcept -> CommitFailReason;

                    static auto withForcedParticipantNotInQuorum(ParticipantId who) noexcept -> CommitFailReason;

                    static auto
                    withNonEligibleServerRequiredForQuorum(NonEligibleServerRequiredForQuorum::CandidateMap) noexcept -> CommitFailReason;

                    // This would have too many `std::size_t` arguments to not be confusing,
                    // so taking the full object instead.
                    static auto
                    withFewerParticipantsThanWriteConcern(FewerParticipantsThanWriteConcern) -> CommitFailReason;

                    static auto from_velocy_pack(velocypack::Slice) -> CommitFailReason;

                    void to_velocy_pack(velocypack::Builder &builder) const;

                    bool operator==(const CommitFailReason &rhs) const {
                        return value == rhs.value;
                    }

                    bool operator!=(const CommitFailReason &rhs) const {
                        return !(rhs == *this);
                    }

                private:
                    template<typename... Args>
                    explicit CommitFailReason(std::in_place_t, Args &&...args)

                    noexcept;
                };

                template<class Inspector>
                auto inspect(Inspector &f, nil::dbms::replication::log::CommitFailReason &x) {
                    if constexpr(Inspector::isLoading)
                    {
                        x = CommitFailReason::from_velocy_pack(f.slice());
                    } else {
                        x.to_velocy_pack(f.builder());
                    }
                    return nil::dbms::inspection::Status::Success{};
                }

                auto
                operator<<(std::ostream &, CommitFailReason::QuorumSizeNotReached::ParticipantInfo) -> std::ostream &;

                auto to_string(CommitFailReason const &) -> std::string;
            }    // namespace log

        }
    }
}    // namespace nil::dbms::replication

namespace nil::dbms {
    template<>
    struct velocypack::Extractor<replication::log_term> {
        static auto extract(velocypack::Slice slice) -> replication::log_term {
            return replication::log_term{slice.getNumericValue<std::uint64_t>()};
        }
    };

    template<>
    struct velocypack::Extractor<replication::log_index> {
        static auto extract(velocypack::Slice slice) -> replication::log_index {
            return replication::log_index{slice.getNumericValue<std::uint64_t>()};
        }
    };

    template<>
    struct velocypack::Extractor<replication::log_id> {
        static auto extract(velocypack::Slice slice) -> replication::log_id {
            return replication::log_id{slice.getNumericValue<std::uint64_t>()};
        }
    };

}    // namespace nil::dbms

template<>
struct fmt::formatter<nil::dbms::replication::log_id> : fmt::formatter<nil::dbms::basics::Identifier> {
};

template<>
struct std::hash<nil::dbms::replication::log_index> {
    [[nodiscard]] auto operator()(nil::dbms::replication::log_index const &v) const

    noexcept -> std::size_t {
        return std::hash<uint64_t>{}(v.value);
    }
};

template<>
struct std::hash<nil::dbms::replication::log_term> {
    [[nodiscard]] auto operator()(nil::dbms::replication::log_term const &v) const

    noexcept -> std::size_t {
        return std::hash<uint64_t>{}(v.value);
    }
};

template<>
struct std::hash<nil::dbms::replication::log_id> {
    [[nodiscard]] auto operator()(nil::dbms::replication::log_id const &v) const

    noexcept -> std::size_t {
        return std::hash<nil::dbms::basics::Identifier>{}(v);
    }
};
