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

#include "streams.hpp"

namespace nil::dbms::replication::streams {

    template<typename Descriptor, typename Type = stream_descriptor_type_t<Descriptor>>
    struct DescriptorValueTag {
        using DescriptorType = Descriptor;
        explicit DescriptorValueTag(Type value) : value(std::move(value)) {
        }
        Type value;
    };

    template<typename... Descriptors>
    struct MultiplexedVariant {
        using VariantType = std::variant<DescriptorValueTag<Descriptors>...>;

        [[nodiscard]] auto variant() & -> VariantType & {
            return _value;
        }
        [[nodiscard]] auto variant() && -> VariantType && {
            return std::move(_value);
        }
        [[nodiscard]] auto variant() const & -> VariantType & {
            return _value;
        }

        template<typename... Args>
        explicit MultiplexedVariant(std::in_place_t, Args &&...args) : _value(std::forward<Args>(args)...) {
        }

    private:
        VariantType _value;
    };

    struct MultiplexedValues {
        template<typename Descriptor, typename Type = stream_descriptor_type_t<Descriptor>>
        static void to_velocy_pack(Type const &v, velocypack::Builder &builder) {
            using PrimaryTag = stream_descriptor_primary_tag_t<Descriptor>;
            using Serializer = typename PrimaryTag::serializer;
            velocypack::ArrayBuilder ab(&builder);
            builder.add(velocypack::Value(PrimaryTag::tag));
            static_assert(std::is_invocable_r_v<void, Serializer, serializer_tag_t<Type>,
                                                std::add_lvalue_reference_t<std::add_const_t<Type>>,
                                                std::add_lvalue_reference_t<velocypack::Builder>>);
            std::invoke(Serializer {}, serializer_tag<Type>, v, builder);
        }

        template<typename... Descriptors>
        static auto from_velocy_pack(velocypack::Slice slice) -> MultiplexedVariant<Descriptors...> {
            TRI_ASSERT(slice.isArray());
            auto [tag, valueSlice] = slice.unpackTuple<StreamTag, velocypack::Slice>();
            return from_velocy_packHelper<MultiplexedVariant<Descriptors...>, Descriptors...>::extract(tag, valueSlice);
        }

    private:
        template<typename ValueType, typename Descriptor, typename... Other>
        struct from_velocy_packHelper {
            static auto extract(StreamTag tag, velocypack::Slice slice) -> ValueType {
                return extractTags(stream_descriptor_tags_t<Descriptor> {}, tag, slice);
            }

            template<typename Tag, typename... Tags>
            static auto extractTags(tag_descriptor_set<Tag, Tags...>, StreamTag tag, velocypack::Slice slice)
                -> ValueType {
                if (Tag::tag == tag) {
                    return extractValue<typename Tag::deserializer>(slice);
                } else if constexpr (sizeof...(Tags) > 0) {
                    return extractTags(tag_descriptor_set<Tags...> {}, tag, slice);
                } else if constexpr (sizeof...(Other) > 0) {
                    return from_velocy_packHelper<ValueType, Other...>::extract(tag, slice);
                } else {
                    std::abort();
                }
            }

            template<typename Deserializer, typename Type = stream_descriptor_type_t<Descriptor>>
            static auto extractValue(velocypack::Slice slice) -> ValueType {
                static_assert(std::is_invocable_r_v<Type, Deserializer, serializer_tag_t<Type>, velocypack::Slice>);
                auto value = std::invoke(Deserializer {}, serializer_tag<Type>, slice);
                return ValueType(std::in_place, std::in_place_type<DescriptorValueTag<Descriptor>>, std::move(value));
            }
        };
    };
}    // namespace nil::dbms::replication::streams
