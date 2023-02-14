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

#include "velocypack/Builder.h"
#include "velocypack/velocypack-aliases.h"

#include <iosfwd>
#include <limits>
#include <memory>
#include <string>

#include "basics/result.h"

namespace nil::dbms {

    typedef std::string ServerID;           // ID of a server
    typedef std::string DatabaseID;         // ID/name of a database
    typedef std::string CollectionID;       // ID of a collection
    typedef std::string ViewID;             // ID of a view
    typedef std::string ShardID;            // ID of a shard
    typedef uint32_t ServerShortID;         // Short ID of a server
    typedef std::string ServerShortName;    // Short name of a server

    class RebootId {
    public:
        explicit constexpr RebootId() noexcept = delete;
        explicit constexpr RebootId(uint64_t rebootId) noexcept : _value(rebootId) {
        }
        [[nodiscard]] uint64_t value() const noexcept {
            return _value;
        }

        [[nodiscard]] bool initialized() const noexcept {
            return value() != 0;
        }

        [[nodiscard]] bool operator==(RebootId other) const noexcept {
            return value() == other.value();
        }
        [[nodiscard]] bool operator!=(RebootId other) const noexcept {
            return value() != other.value();
        }
        [[nodiscard]] bool operator<(RebootId other) const noexcept {
            return value() < other.value();
        }
        [[nodiscard]] bool operator>(RebootId other) const noexcept {
            return value() > other.value();
        }
        [[nodiscard]] bool operator<=(RebootId other) const noexcept {
            return value() <= other.value();
        }
        [[nodiscard]] bool operator>=(RebootId other) const noexcept {
            return value() >= other.value();
        }

        [[nodiscard]] static constexpr RebootId max() noexcept {
            return RebootId {std::numeric_limits<decltype(_value)>::max()};
        }

        std::ostream &print(std::ostream &o) const;

    private:
        uint64_t _value {};
    };

    template<class Inspector>
    auto inspect(Inspector &f, RebootId &x) {
        if constexpr (Inspector::isLoading) {
            auto v = uint64_t {0};
            auto res = f.apply(v);
            if (res.ok()) {
                x = RebootId {v};
            }
            return res;
        } else {
            auto v = x.value();
            return f.apply(v);
        }
    }

    namespace velocypack {
        class Builder;
        class Slice;
    }    // namespace velocypack

    struct AnalyzersRevision {
    public:
        using Revision = uint64_t;
        using Ptr = std::shared_ptr<AnalyzersRevision const>;

        static constexpr Revision LATEST = std::numeric_limits<uint64_t>::max();
        static constexpr Revision MIN = 0;

        AnalyzersRevision(AnalyzersRevision const &) = delete;
        AnalyzersRevision &operator=(AnalyzersRevision const &) = delete;

        Revision getRevision() const noexcept {
            return _revision;
        }

        Revision getBuildingRevision() const noexcept {
            return _buildingRevision;
        }

        ServerID const &getServerID() const noexcept {
            return _serverID;
        }

        RebootId const &getRebootID() const noexcept {
            return _rebootID;
        }

        void toVelocyPack(VPackBuilder &builder) const;

        static Ptr fromVelocyPack(VPackSlice const &slice, std::string &error);

        static Ptr getEmptyRevision();

    private:
        AnalyzersRevision(Revision revision, Revision buildingRevision, ServerID &&serverID, uint64_t rebootID) noexcept
            :
            _revision(revision),
            _buildingRevision(buildingRevision), _serverID(std::move(serverID)), _rebootID(rebootID) {
        }

        Revision _revision;
        Revision _buildingRevision;
        ServerID _serverID;
        RebootId _rebootID;
    };

    /// @brief Analyzers revisions used in query.
    /// Stores current database revision
    /// and _system database revision (analyzers from _system are accessible from
    /// other databases) If at some point we will decide to allow cross-database
    /// anayzer usage this could became more complicated. But for now  we keep it
    /// simple - store just two members
    struct QueryAnalyzerRevisions {
        constexpr QueryAnalyzerRevisions(AnalyzersRevision::Revision current, AnalyzersRevision::Revision system) :
            currentDbRevision(current), systemDbRevision(system) {
        }

        QueryAnalyzerRevisions() = default;
        QueryAnalyzerRevisions(QueryAnalyzerRevisions const &) = default;
        QueryAnalyzerRevisions &operator=(QueryAnalyzerRevisions const &) = default;

        void toVelocyPack(VPackBuilder &builder) const;
        nil::dbms::Result fromVelocyPack(dbms::velocypack::Slice slice);

        bool isDefault() const noexcept {
            return currentDbRevision == AnalyzersRevision::MIN && systemDbRevision == AnalyzersRevision::MIN;
        }

        bool operator==(QueryAnalyzerRevisions const &other) const noexcept {
            return currentDbRevision == other.currentDbRevision && systemDbRevision == other.systemDbRevision;
        }

        std::ostream &print(std::ostream &o) const;

        bool operator!=(QueryAnalyzerRevisions const &other) const noexcept {
            return !(*this == other);
        }

        /// @brief Gets analyzers revision to be used with specified database
        /// @param vocbase database name
        /// @return analyzers revision
        AnalyzersRevision::Revision getVocbaseRevision(std::string_view vocbase) const noexcept;

        static QueryAnalyzerRevisions QUERY_LATEST;

    private:
        AnalyzersRevision::Revision currentDbRevision {AnalyzersRevision::MIN};
        AnalyzersRevision::Revision systemDbRevision {AnalyzersRevision::MIN};
    };

    template<>
    struct velocypack::Extractor<nil::dbms::RebootId> {
        static auto extract(velocypack::Slice slice) -> RebootId {
            return RebootId {slice.getNumericValue<std::size_t>()};
        }
    };

    std::ostream &operator<<(std::ostream &o, RebootId const &r);
    std::ostream &operator<<(std::ostream &o, QueryAnalyzerRevisions const &r);

}    // namespace nil::replication_sdk::cluster
