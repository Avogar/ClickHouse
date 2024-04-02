#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnDynamic.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void SerializationDynamicElement::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData &) const
{
    /// We will need stream for Dynamic structure during deserialization.
    settings.path.push_back(Substream::DynamicStructure);
    callback(settings.path);
    settings.path.pop_back();

    /// We don't know if we have actually have this variant in Dynamic column,
    /// so we cannot enumerate variant streams.
}

void SerializationDynamicElement::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationDynamicElement");
}

void SerializationDynamicElement::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationDynamicElement");
}

struct DeserializeBinaryBulkStateDynamicElement : public ISerialization::DeserializeBinaryBulkState
{
    SerializationDynamic::DynamicStructureSerializationVersion structure_version;
    SerializationPtr variant_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr variant_element_state;

    explicit DeserializeBinaryBulkStateDynamicElement(UInt64 version) : structure_version(version) {}
};

void SerializationDynamicElement::deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DynamicStructure);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Dynamic column structure during serialization of binary bulk state prefix");

    /// Read structure serialization version.
    UInt64 structure_version;
    readBinaryLittleEndian(structure_version, *stream);
    auto dynamic_element_state = std::make_shared<DeserializeBinaryBulkStateDynamicElement>(structure_version);
    /// Read internal Variant type name.
    String data_type_name;
    readStringBinary(data_type_name, *stream);
    auto variant_type = DataTypeFactory::instance().get(data_type_name);
    /// Check if we actually have required element in the Variant.
    if (auto global_discr = assert_cast<const DataTypeVariant &>(*variant_type).tryGetVariantDiscriminator(dynamic_element_name))
    {
        dynamic_element_state->variant_serialization = std::make_shared<SerializationVariantElement>(nested_serialization, dynamic_element_name, *global_discr);
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_element_state->variant_element_state);
        settings.path.pop_back();
    }

    state = std::move(dynamic_element_state);
}

void SerializationDynamicElement::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationDynamicElement");
}

void SerializationDynamicElement::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * dynamic_element_state = checkAndGetState<DeserializeBinaryBulkStateDynamicElement>(state);

    if (dynamic_element_state->variant_serialization)
    {
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, dynamic_element_state->variant_element_state, cache);
        settings.path.pop_back();
    }
    else
    {
        auto mutable_column = result_column->assumeMutable();
        mutable_column->insertManyDefaults(limit);
        result_column = std::move(mutable_column);
    }
}

}
