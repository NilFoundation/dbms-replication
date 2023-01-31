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

#include <nil/dbms/replication/log/log_common.hpp>

#include <velocypack/Builder.h>
#include <velocypack/Slice.h>
#include <velocypack/Value.h>

#include <basics/static_strings.h>
#include <basics/string_utils.h>
#include <basics/velocypack_helper.h>
#include <basics/debugging.h>
#include <inspection/vpack.h>

#include <chrono>
#include <utility>
#include <fmt/core.h>

using namespace nil::dbms;
using namespace nil::dbms::replication;

auto log_index::operator+(std::uint64_t delta) const -> log_index {
    return log_index(this->value + delta);
}

log_index::operator velocypack::Value() const noexcept {
    return velocypack::Value(value);
}

auto replication::operator<<(std::ostream &os, log_index idx) -> std::ostream & {
    return os << idx.value;
}

auto log_index::saturatedDecrement(uint64_t delta) const noexcept -> log_index {
    if (value > delta) {
        return log_index {value - delta};
    }

    return log_index {0};
}

log_term::operator velocypack::Value() const noexcept {
    return velocypack::Value(value);
}

auto replication::operator<<(std::ostream &os, log_term term) -> std::ostream & {
    return os << term.value;
}

auto log_id::fromString(std::string_view name) noexcept -> std::optional<log_id> {
    if (std::all_of(name.begin(), name.end(), [](char c) { return isdigit(c); })) {
        using namespace basics::StringUtils;
        return log_id {uint64(name)};
    }
    return std::nullopt;
}

[[nodiscard]] log_id::operator velocypack::Value() const noexcept {
    return velocypack::Value(id());
}

auto replication::to_string(log_id log_id) -> std::string {
    return std::to_string(log_id.id());
}

auto replication::to_string(log_term term) -> std::string {
    return std::to_string(term.value);
}

auto replication::to_string(log_index index) -> std::string {
    return std::to_string(index.value);
}

void replication::term_index_pair::to_velocy_pack(velocypack::Builder &builder) const {
    serialize(builder, *this);
}

auto replication::term_index_pair::from_velocy_pack(velocypack::Slice slice) -> term_index_pair {
    return velocypack::deserialize<term_index_pair>(slice);
}

replication::term_index_pair::term_index_pair(log_term term, log_index index) noexcept : term(term), index(index) {
    // Index 0 has always term 0, and it is the only index with that term.
    // FIXME this should be an if and only if
    TRI_ASSERT((index != log_index {0}) || (term == log_term {0}));
}

auto replication::operator<<(std::ostream &os, term_index_pair pair) -> std::ostream & {
    return os << '(' << pair.term << ':' << pair.index << ')';
}

log_range::log_range(log_index from, log_index to) noexcept : from(from), to(to) {
    TRI_ASSERT(from <= to);
}

auto log_range::empty() const noexcept -> bool {
    return from == to;
}

auto log_range::count() const noexcept -> std::size_t {
    return to.value - from.value;
}

auto log_range::contains(log_index idx) const noexcept -> bool {
    return from <= idx && idx < to;
}

auto replication::operator<<(std::ostream &os, log_range const &r) -> std::ostream & {
    return os << "[" << r.from << ", " << r.to << ")";
}

auto replication::intersect(log_range a, log_range b) noexcept -> log_range {
    auto max_from = std::max(a.from, b.from);
    auto min_to = std::min(a.to, b.to);
    if (max_from > min_to) {
        return {log_index {0}, log_index {0}};
    } else {
        return {max_from, min_to};
    }
}

auto replication::to_string(log_range const &r) -> std::string {
    return basics::StringUtils::concatT("[", r.from, ", ", r.to, ")");
}

auto log_range::end() const noexcept -> log_range::Iterator {
    return Iterator {to};
}
auto log_range::begin() const noexcept -> log_range::Iterator {
    return Iterator {from};
}

auto log_range::Iterator::operator++() noexcept -> log_range::Iterator & {
    current = current + 1;
    return *this;
}

auto log_range::Iterator::operator++(int) noexcept -> log_range::Iterator {
    auto idx = current;
    current = current + 1;
    return Iterator(idx);
}

auto log_range::Iterator::operator*() const noexcept -> log_index {
    return current;
}
auto log_range::Iterator::operator->() const noexcept -> log_index const * {
    return &current;
}

template<typename... Args>
log::CommitFailReason::CommitFailReason(std::in_place_t, Args &&...args) noexcept :
    value(std::forward<Args>(args)...) {
}

auto log::CommitFailReason::withNothingToCommit() noexcept -> CommitFailReason {
    return CommitFailReason(std::in_place, NothingToCommit {});
}

auto log::CommitFailReason::withQuorumSizeNotReached(QuorumSizeNotReached::who_type who,
                                                                term_index_pair spearhead) noexcept -> CommitFailReason {
    return CommitFailReason(std::in_place, QuorumSizeNotReached {std::move(who), spearhead});
}

auto log::CommitFailReason::withForcedParticipantNotInQuorum(ParticipantId who) noexcept
    -> CommitFailReason {
    return CommitFailReason(std::in_place, ForcedParticipantNotInQuorum {std::move(who)});
}

auto log::CommitFailReason::withNonEligibleServerRequiredForQuorum(
    NonEligibleServerRequiredForQuorum::CandidateMap candidates) noexcept -> CommitFailReason {
    return CommitFailReason(std::in_place, NonEligibleServerRequiredForQuorum {std::move(candidates)});
}

namespace {
    constexpr static const char *ReasonFieldName = "reason";
    constexpr static const char *NothingToCommitEnum = "NothingToCommit";
    constexpr static const char *QuorumSizeNotReachedEnum = "QuorumSizeNotReached";
    constexpr static const char *ForcedParticipantNotInQuorumEnum = "ForcedParticipantNotInQuorum";
    constexpr static const char *NonEligibleServerRequiredForQuorumEnum = "NonEligibleServerRequiredForQuorum";
    constexpr static const char *WhoFieldName = "who";
    constexpr static const char *CandidatesFieldName = "candidates";
    constexpr static const char *NonEligibleNotAllowedInQuorum = "notAllowedInQuorum";
    constexpr static const char *NonEligibleWrongTerm = "wrongTerm";
    constexpr static const char *IsFailedFieldName = "isFailed";
    constexpr static const char *IsAllowedInQuorumFieldName = "isAllowedInQuorum";
    constexpr static const char *LastAcknowledgedFieldName = "lastAcknowledged";
    constexpr static const char *SpearheadFieldName = "spearhead";
}    // namespace

auto log::CommitFailReason::NothingToCommit::from_velocy_pack(velocypack::Slice s) -> NothingToCommit {
    TRI_ASSERT(s.get(ReasonFieldName).isString()) << "Expected string, found: " << s.toJson();
    TRI_ASSERT(s.get(ReasonFieldName).toString() == NothingToCommitEnum)
        << "Expected string `" << NothingToCommitEnum << "`, found: " << s.stringView();
    return {};
}

void log::CommitFailReason::NothingToCommit::to_velocy_pack(velocypack::Builder &builder) const {
    VPackObjectBuilder obj(&builder);
    builder.add(ReasonFieldName, VPackValue(NothingToCommitEnum));
}

auto log::CommitFailReason::QuorumSizeNotReached::from_velocy_pack(velocypack::Slice s)
    -> QuorumSizeNotReached {
    TRI_ASSERT(s.get(ReasonFieldName).isString()) << "Expected string, found: " << s.toJson();
    TRI_ASSERT(s.get(ReasonFieldName).isEqualString(QuorumSizeNotReachedEnum))
        << "Expected string `" << QuorumSizeNotReachedEnum << "`, found: " << s.stringView();
    TRI_ASSERT(s.get(WhoFieldName).isObject()) << "Expected object, found: " << s.toJson();
    auto result = QuorumSizeNotReached();
    for (auto const &[participantIdSlice, participantInfoSlice] : VPackObjectIterator(s.get(WhoFieldName))) {
        auto const participantId = participantIdSlice.stringView();
        result.who.emplace(participantId, ParticipantInfo::from_velocy_pack(participantInfoSlice));
    }
    result.spearhead = velocypack::deserialize<term_index_pair>(s.get(SpearheadFieldName));
    return result;
}

void log::CommitFailReason::QuorumSizeNotReached::to_velocy_pack(velocypack::Builder &builder) const {
    VPackObjectBuilder obj(&builder);
    builder.add(ReasonFieldName, VPackValue(QuorumSizeNotReachedEnum));
    {
        builder.add(VPackValue(WhoFieldName));
        VPackObjectBuilder objWho(&builder);

        for (auto const &[participantId, participantInfo] : who) {
            builder.add(VPackValue(participantId));
            participantInfo.to_velocy_pack(builder);
        }
    }
    {
        builder.add(VPackValue(SpearheadFieldName));
        serialize(builder, spearhead);
    }
}

auto log::CommitFailReason::QuorumSizeNotReached::ParticipantInfo::from_velocy_pack(velocypack::Slice s)
    -> ParticipantInfo {
    TRI_ASSERT(s.get(IsFailedFieldName).isBool())
        << "Expected bool in field `" << IsFailedFieldName << "` in " << s.toJson();
    return {
        .isFailed = s.get(IsFailedFieldName).getBool(),
        .isAllowedInQuorum = s.get(IsAllowedInQuorumFieldName).getBool(),
        .lastAcknowledged = velocypack::deserialize<term_index_pair>(s.get(LastAcknowledgedFieldName)),
    };
}

void log::CommitFailReason::QuorumSizeNotReached::ParticipantInfo::to_velocy_pack(
    velocypack::Builder &builder) const {
    VPackObjectBuilder obj(&builder);
    builder.add(IsFailedFieldName, VPackValue(isFailed));
    builder.add(IsAllowedInQuorumFieldName, VPackValue(isAllowedInQuorum));
    {
        builder.add(VPackValue(LastAcknowledgedFieldName));
        serialize(builder, lastAcknowledged);
    }
}

auto log::operator<<(std::ostream &ostream, CommitFailReason::QuorumSizeNotReached::ParticipantInfo pInfo)
    -> std::ostream & {
    ostream << "{ ";
    ostream << std::boolalpha;
    if (pInfo.isAllowedInQuorum) {
        ostream << "isAllowedInQuorum: " << pInfo.isAllowedInQuorum;
    } else {
        ostream << "lastAcknowledgedEntry: " << pInfo.lastAcknowledged;
        if (pInfo.isFailed) {
            ostream << ", isFailed: " << pInfo.isFailed;
        }
    }
    ostream << " }";
    return ostream;
}

auto log::CommitFailReason::ForcedParticipantNotInQuorum::from_velocy_pack(velocypack::Slice s)
    -> ForcedParticipantNotInQuorum {
    TRI_ASSERT(s.get(ReasonFieldName).isString()) << "Expected string, found: " << s.toJson();
    TRI_ASSERT(s.get(ReasonFieldName).isEqualString(ForcedParticipantNotInQuorumEnum))
        << "Expected string `" << ForcedParticipantNotInQuorumEnum << "`, found: " << s.stringView();
    TRI_ASSERT(s.get(WhoFieldName).isString()) << "Expected string, found: " << s.toJson();
    return {s.get(WhoFieldName).toString()};
}

void log::CommitFailReason::ForcedParticipantNotInQuorum::to_velocy_pack(velocypack::Builder &builder) const {
    VPackObjectBuilder obj(&builder);
    builder.add(ReasonFieldName, VPackValue(ForcedParticipantNotInQuorumEnum));
    builder.add(WhoFieldName, VPackValue(who));
}

void log::CommitFailReason::NonEligibleServerRequiredForQuorum::to_velocy_pack(
    velocypack::Builder &builder) const {
    VPackObjectBuilder obj(&builder);
    builder.add(ReasonFieldName, VPackValue(NonEligibleServerRequiredForQuorumEnum));
    builder.add(VPackValue(CandidatesFieldName));
    VPackObjectBuilder canObject(&builder);
    for (auto const &[p, why] : candidates) {
        builder.add(p, VPackValue(to_string(why)));
    }
}

auto log::CommitFailReason::NonEligibleServerRequiredForQuorum::to_string(
    log::CommitFailReason::NonEligibleServerRequiredForQuorum::Why why) noexcept -> std::string_view {
    switch (why) {
        case kNotAllowedInQuorum:
            return NonEligibleNotAllowedInQuorum;
        case kWrongTerm:
            return NonEligibleWrongTerm;
        default:
            TRI_ASSERT(false);
            return "(unknown)";
    }
}

auto log::CommitFailReason::NonEligibleServerRequiredForQuorum::from_velocy_pack(velocypack::Slice s)
    -> NonEligibleServerRequiredForQuorum {
    TRI_ASSERT(s.get(ReasonFieldName).isEqualString(NonEligibleServerRequiredForQuorumEnum))
        << "Expected string `" << NonEligibleServerRequiredForQuorumEnum << "`, found: " << s.stringView();
    CandidateMap candidates;
    for (auto const &[key, value] : velocypack::ObjectIterator(s.get(CandidatesFieldName))) {
        if (value.isEqualString(NonEligibleNotAllowedInQuorum)) {
            candidates[key.copyString()] = kNotAllowedInQuorum;
        } else if (value.isEqualString(NonEligibleWrongTerm)) {
            candidates[key.copyString()] = kWrongTerm;
        }
    }
    return NonEligibleServerRequiredForQuorum {std::move(candidates)};
}

auto log::CommitFailReason::from_velocy_pack(velocypack::Slice s) -> CommitFailReason {
    auto reason = s.get(ReasonFieldName).stringView();
    if (reason == NothingToCommitEnum) {
        return CommitFailReason {std::in_place, NothingToCommit::from_velocy_pack(s)};
    } else if (reason == QuorumSizeNotReachedEnum) {
        return CommitFailReason {std::in_place, QuorumSizeNotReached::from_velocy_pack(s)};
    } else if (reason == ForcedParticipantNotInQuorumEnum) {
        return CommitFailReason {std::in_place, ForcedParticipantNotInQuorum::from_velocy_pack(s)};
    } else if (reason == NonEligibleServerRequiredForQuorumEnum) {
        return CommitFailReason {std::in_place, NonEligibleServerRequiredForQuorum::from_velocy_pack(s)};
    } else {
        THROW_DBMS_EXCEPTION_MESSAGE(TRI_ERROR_BAD_PARAMETER,
                                     basics::StringUtils::concatT("CommitFailReason `", reason, "` unknown."));
    }
}

void log::CommitFailReason::to_velocy_pack(velocypack::Builder &builder) const {
    std::visit([&](auto const &v) { v.to_velocy_pack(builder); }, value);
}

auto log::CommitFailReason::withFewerParticipantsThanWriteConcern(
    FewerParticipantsThanWriteConcern fewerParticipantsThanWriteConcern) -> log::CommitFailReason {
    auto result = CommitFailReason();
    result.value = fewerParticipantsThanWriteConcern;
    return result;
}

auto log::to_string(CommitFailReason const &r) -> std::string {
    struct ToStringVisitor {
        auto operator()(CommitFailReason::NothingToCommit const &) -> std::string {
            return "Nothing to commit";
        }
        auto operator()(CommitFailReason::QuorumSizeNotReached const &reason) -> std::string {
            auto stream = std::stringstream();
            stream << "Required quorum size not yet reached. ";
            stream << "The leader's spearhead is at " << reason.spearhead << ". ";
            stream << "Participants who aren't currently contributing to the "
                      "spearhead are ";
            // ADL cannot find this operator here.
            nil::dbms::operator<<(stream, reason.who);
            return stream.str();
        }
        auto operator()(CommitFailReason::ForcedParticipantNotInQuorum const &reason) -> std::string {
            return "Forced participant not in quorum. Participant " + reason.who;
        }
        auto operator()(CommitFailReason::NonEligibleServerRequiredForQuorum const &reason) -> std::string {
            auto result = std::string {"A non-eligible server is required to reach a quorum: "};
            for (auto const &[pid, why] : reason.candidates) {
                result += basics::StringUtils::concatT(" ", pid, ": ", why);
            }
            return result;
        }
        auto operator()(CommitFailReason::FewerParticipantsThanWriteConcern const &reason) {
            return fmt::format("Fewer participants than effectove write concern. Have {} ",
                               "participants and effectiveWriteConcern={}.", reason.numParticipants,
                               reason.effectiveWriteConcern);
        }
    };

    return std::visit(ToStringVisitor {}, r.value);
}

void replication::ParticipantFlags::to_velocy_pack(velocypack::Builder &builder) const {
    serialize(builder, *this);
}

auto replication::ParticipantFlags::from_velocy_pack(velocypack::Slice s) -> ParticipantFlags {
    return velocypack::deserialize<ParticipantFlags>(s);
}

auto replication::operator<<(std::ostream &os, ParticipantFlags const &f) -> std::ostream & {
    os << "{ ";
    if (f.forced) {
        os << "forced ";
    }
    if (f.allowedAsLeader) {
        os << "allowedAsLeader ";
    }
    if (f.allowedInQuorum) {
        os << "allowedInQuorum ";
    }
    return os << "}";
}

auto log::CommitFailReason::FewerParticipantsThanWriteConcern::from_velocy_pack(velocypack::Slice)
    -> log::CommitFailReason::FewerParticipantsThanWriteConcern {
    auto result = log::CommitFailReason::FewerParticipantsThanWriteConcern();

    return result;
}

void log::CommitFailReason::FewerParticipantsThanWriteConcern::to_velocy_pack(
    velocypack::Builder &builder) const {
    VPackObjectBuilder obj(&builder);
    builder.add(StaticStrings::EffectiveWriteConcern, VPackValue(effectiveWriteConcern));
}

global_log_identifier::global_log_identifier(const std::string &database, log_id id) : database(std::move(database)), id(id) {
}
