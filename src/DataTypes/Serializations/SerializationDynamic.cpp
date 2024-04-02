#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnDynamic.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/castColumn.h>
#include <Formats/EscapingRuleUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

void SerializationDynamic::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::DynamicStructure);
    callback(settings.path);
    settings.path.pop_back();

    const auto * column_dynamic = data.column ? &assert_cast<const ColumnDynamic &>(*data.column) : nullptr;

    /// If column is nullptr, nothing to enumerate as we don't have any variants.
    if (!column_dynamic)
        return;

    const auto & variant_info = column_dynamic->getVariantInfo();
    auto variant_serialization = variant_info.variant_type->getDefaultSerialization();

    settings.path.push_back(Substream::DynamicData);
    auto variant_data = SubstreamData(variant_serialization)
                         .withType(variant_info.variant_type)
                         .withColumn(column_dynamic->getVariantColumnPtr())
                         .withSerializationInfo(data.serialization_info);
    settings.path.back().data = variant_data;
    variant_serialization->enumerateStreams(settings, callback, variant_data);
    settings.path.pop_back();
}

SerializationDynamic::DynamicStructureSerializationVersion::DynamicStructureSerializationVersion(UInt64 version) : value(static_cast<Value>(version))
{
    checkVersion(version);
}

void SerializationDynamic::DynamicStructureSerializationVersion::checkVersion(UInt64 version)
{
    if (version != VariantTypeName)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for Dynamic structure serialization.");
}

struct SerializeBinaryBulkStateDynamic : public ISerialization::SerializeBinaryBulkState
{
    SerializationDynamic::DynamicStructureSerializationVersion structure_version;
    DataTypePtr variant_type;
    SerializationPtr variant_serialization;
    ISerialization::SerializeBinaryBulkStatePtr variant_state;

    explicit SerializeBinaryBulkStateDynamic(UInt64 structure_version_) : structure_version(structure_version_) {}
};

struct DeserializeBinaryBulkStateDynamic : public ISerialization::DeserializeBinaryBulkState
{
    SerializationDynamic::DynamicStructureSerializationVersion structure_version;
    DataTypePtr variant_type;
    SerializationPtr variant_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr variant_state;

    explicit DeserializeBinaryBulkStateDynamic(UInt64 structure_version_) : structure_version(structure_version_) {}
};

void SerializationDynamic::serializeBinaryBulkStatePrefix(
    const DB::IColumn & column,
    DB::ISerialization::SerializeBinaryBulkSettings & settings,
    DB::ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_dynamic = assert_cast<const ColumnDynamic &>(column);
    const auto & variant_info = column_dynamic.getVariantInfo();

    settings.path.push_back(Substream::DynamicStructure);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Dynamic column structure during serialization of binary bulk state prefix");

    /// Write structure serialization version.
    UInt64 structure_version = DynamicStructureSerializationVersion::Value::VariantTypeName;
    writeBinaryLittleEndian(structure_version, *stream);
    /// Write internal Variant type name.
    writeStringBinary(variant_info.variant_type->getName(), *stream);
    auto dynamic_state = std::make_shared<SerializeBinaryBulkStateDynamic>(structure_version);
    dynamic_state->variant_type = variant_info.variant_type;
    dynamic_state->variant_serialization = variant_info.variant_type->getDefaultSerialization();

    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->serializeBinaryBulkStatePrefix(column_dynamic.getVariantColumn(), settings, dynamic_state->variant_state);
    settings.path.pop_back();

    state = std::move(dynamic_state);
}

void SerializationDynamic::deserializeBinaryBulkStatePrefix(
    DB::ISerialization::DeserializeBinaryBulkSettings & settings, DB::ISerialization::DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::DynamicStructure);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Dynamic column structure during serialization of binary bulk state prefix");

    /// Read structure serialization version.
    UInt64 structure_version;
    readBinaryLittleEndian(structure_version, *stream);
    auto dynamic_state = std::make_shared<DeserializeBinaryBulkStateDynamic>(structure_version);
    /// Read internal Variant type name.
    String data_type_name;
    readStringBinary(data_type_name, *stream);
    dynamic_state->variant_type = DataTypeFactory::instance().get(data_type_name);
    dynamic_state->variant_serialization = dynamic_state->variant_type->getDefaultSerialization();
    if (!isVariant(dynamic_state->variant_type))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect type of Dynamic nested column, expected Variant, got {}", dynamic_state->variant_type->getName());

    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_state->variant_state);
    settings.path.pop_back();

    state = std::move(dynamic_state);
}

void SerializationDynamic::serializeBinaryBulkStateSuffix(
    DB::ISerialization::SerializeBinaryBulkSettings & settings, DB::ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    auto * dynamic_state = checkAndGetState<SerializeBinaryBulkStateDynamic>(state);

    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->serializeBinaryBulkStateSuffix(settings, dynamic_state->variant_state);
    settings.path.pop_back();
}

void SerializationDynamic::serializeBinaryBulkWithMultipleStreams(
    const DB::IColumn & column,
    size_t offset,
    size_t limit,
    DB::ISerialization::SerializeBinaryBulkSettings & settings,
    DB::ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_dynamic = assert_cast<const ColumnDynamic &>(column);
    auto * dynamic_state = checkAndGetState<SerializeBinaryBulkStateDynamic>(state);
    const auto & variant_info = column_dynamic.getVariantInfo();

    if (!variant_info.variant_type->equals(*dynamic_state->variant_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatch of internal columns of Dynamic. Expected: {}, Got: {}", dynamic_state->variant_type->getName(), variant_info.variant_type->getName());

    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->serializeBinaryBulkWithMultipleStreams(column_dynamic.getVariantColumn(), offset, limit, settings, dynamic_state->variant_state);
    settings.path.pop_back();
}

void SerializationDynamic::deserializeBinaryBulkWithMultipleStreams(
    DB::ColumnPtr & column,
    size_t limit,
    DB::ISerialization::DeserializeBinaryBulkSettings & settings,
    DB::ISerialization::DeserializeBinaryBulkStatePtr & state,
    DB::ISerialization::SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    auto * dynamic_state = checkAndGetState<DeserializeBinaryBulkStateDynamic>(state);

    if (mutable_column->empty())
        mutable_column = ColumnDynamic::create(dynamic_state->variant_type->createColumn(), dynamic_state->variant_type);

    auto & column_dynamic = assert_cast<ColumnDynamic &>(*mutable_column);
    const auto & variant_info = column_dynamic.getVariantInfo();
    if (!variant_info.variant_type->equals(*dynamic_state->variant_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatch of internal columns of Dynamic. Expected: {}, Got: {}", dynamic_state->variant_type->getName(), variant_info.variant_type->getName());

    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(column_dynamic.getVariantColumnPtr(), limit, settings, dynamic_state->variant_state, cache);
    settings.path.pop_back();

    column = std::move(mutable_column);
}

void SerializationDynamic::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    UInt8 null_bit = field.isNull();
    writeBinary(null_bit, ostr);
    if (null_bit)
        return;

    auto field_type = applyVisitor(FieldToDataType(), field);
    auto field_type_name = field_type->getName();
    writeVarUInt(field_type_name.size(), ostr);
    writeString(field_type_name, ostr);
    field_type->getDefaultSerialization()->serializeBinary(field, ostr, settings);
}

void SerializationDynamic::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    UInt8 null_bit;
    readBinary(null_bit, istr);
    if (null_bit)
    {
        field = Null();
        return;
    }

    size_t field_type_name_size;
    readVarUInt(field_type_name_size, istr);
    String field_type_name(field_type_name_size, 0);
    istr.readStrict(field_type_name.data(), field_type_name_size);
    auto field_type = DataTypeFactory::instance().get(field_type_name);
    field_type->getDefaultSerialization()->deserializeBinary(field, istr, settings);
}

void SerializationDynamic::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    const auto & variant_info = dynamic_column.getVariantInfo();
    const auto & variant_column = dynamic_column.getVariantColumn();
    auto global_discr = variant_column.globalDiscriminatorAt(row_num);

    UInt8 null_bit = global_discr == ColumnVariant::NULL_DISCRIMINATOR;
    writeBinary(null_bit, ostr);
    if (null_bit)
        return;

    const auto & variant_type = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariant(global_discr);
    const auto & variant_type_name = variant_info.variant_names[global_discr];
    writeVarUInt(variant_type_name.size(), ostr);
    writeString(variant_type_name, ostr);
    variant_type->getDefaultSerialization()->serializeBinary(variant_column.getVariantByGlobalDiscriminator(global_discr), variant_column.offsetAt(row_num), ostr, settings);
}

template <typename DeserializeFunc>
static void deserializeVariant(
    ColumnVariant & variant_column,
    const DataTypePtr & variant_type,
    ColumnVariant::Discriminator global_discr,
    ReadBuffer & istr,
    DeserializeFunc deserialize)
{
    auto & variant = variant_column.getVariantByGlobalDiscriminator(global_discr);
    deserialize(*variant_type->getDefaultSerialization(), variant, istr);
    variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(global_discr));
    variant_column.getOffsets().push_back(variant.size() - 1);
}

void SerializationDynamic::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto & dynamic_column = assert_cast<ColumnDynamic &>(column);
    UInt8 null_bit;
    readBinary(null_bit, istr);
    if (null_bit)
    {
        dynamic_column.insertDefault();
        return;
    }

    size_t variant_type_name_size;
    readVarUInt(variant_type_name_size, istr);
    String variant_type_name(variant_type_name_size, 0);
    istr.readStrict(variant_type_name.data(), variant_type_name_size);

    const auto & variant_info = dynamic_column.getVariantInfo();
    auto it = variant_info.variant_name_to_discriminator.find(variant_type_name);
    if (it != variant_info.variant_name_to_discriminator.end())
    {
        const auto & variant_type = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariant(it->second);
        deserializeVariant(dynamic_column.getVariantColumn(), variant_type, it->second, istr, [&settings](const ISerialization & serialization, IColumn & variant, ReadBuffer & buf){ serialization.deserializeBinary(variant, buf, settings); });
        return;
    }

    /// We don't have this variant yet. Let's try to add it.
    auto variant_type = DataTypeFactory::instance().get(variant_type_name);
    if (dynamic_column.addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator.at(variant_type_name);
        deserializeVariant(dynamic_column.getVariantColumn(), variant_type, discr, istr, [&settings](const ISerialization & serialization, IColumn & variant, ReadBuffer & buf){ serialization.deserializeBinary(variant, buf, settings); });
        return;
    }

    /// We reached maximum number of variants and couldn't add new variant.
    /// This case should be really rare in real use cases.
    /// We should always be able to add String variant and insert value as String.
    dynamic_column.addStringVariant();
    auto tmp_variant_column = variant_type->createColumn();
    variant_type->getDefaultSerialization()->deserializeBinary(*tmp_variant_column, istr, settings);
    auto string_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    auto & variant_column = dynamic_column.getVariantColumn();
    variant_column.insertIntoVariantFrom(variant_info.variant_name_to_discriminator.at("String"), *string_column, 0);
}

void SerializationDynamic::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeTextCSV(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

template <typename ReadFieldFunc, typename TryDeserializeVariantFunc, typename DeserializeVariant>
static void deserializeTextImpl(
    IColumn & column,
    ReadBuffer & istr,
    const FormatSettings & settings,
    ReadFieldFunc read_field,
    FormatSettings::EscapingRule escaping_rule,
    TryDeserializeVariantFunc try_deserialize_variant,
    DeserializeVariant deserialize_variant)
{
    auto & dynamic_column = assert_cast<ColumnDynamic &>(column);
    auto & variant_column = dynamic_column.getVariantColumn();
    const auto & variant_info = dynamic_column.getVariantInfo();
    String field = read_field(istr);
    auto field_buf = std::make_unique<ReadBufferFromString>(field);
    JSONInferenceInfo json_info;
    auto variant_type = tryInferDataTypeByEscapingRule(field, settings, escaping_rule, &json_info);
    if (escaping_rule == FormatSettings::EscapingRule::JSON)
        transformFinalInferredJSONTypeIfNeeded(variant_type, settings, &json_info);

    if (checkIfTypeIsComplete(variant_type) && dynamic_column.addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator.at(variant_type->getName());
        deserializeVariant(dynamic_column.getVariantColumn(), variant_type, discr, *field_buf, deserialize_variant);
        return;
    }

    /// We couldn't infer type or add new variant. Try to insert field into current variants.
    field_buf = std::make_unique<ReadBufferFromString>(field);
    if (try_deserialize_variant(*variant_info.variant_type->getDefaultSerialization(), variant_column, *field_buf))
        return;

    /// We couldn't insert field into any existing variant, add String variant and read value as String.
    dynamic_column.addStringVariant();

    if (escaping_rule == FormatSettings::EscapingRule::Quoted && (field.size() < 2 || field.front() != '\'' || field.back() != '\''))
        field = "'" + field + "'";

    field_buf = std::make_unique<ReadBufferFromString>(field);
    auto string_discr = variant_info.variant_name_to_discriminator.at("String");
    deserializeVariant(dynamic_column.getVariantColumn(), std::make_shared<DataTypeString>(), string_discr, *field_buf, deserialize_variant);
}

void SerializationDynamic::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [&settings](ReadBuffer & buf)
    {
        String field;
        readCSVField(field, buf, settings.csv);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextCSV(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextCSV(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::CSV, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextCSV(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeTextEscaped(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

void SerializationDynamic::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readEscapedString(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextEscaped(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextEscaped(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Escaped, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextEscaped(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeTextQuoted(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

void SerializationDynamic::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readQuotedField(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextQuoted(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextQuoted(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Quoted, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextQuoted(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextQuoted(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeTextJSON(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

void SerializationDynamic::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [&settings](ReadBuffer & buf)
    {
        String field;
        readJSONField(field, buf, settings.json);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextJSON(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextJSON(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::JSON, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextJSON(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeTextRaw(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

void SerializationDynamic::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readString(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextRaw(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextRaw(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Raw, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextRaw(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextRaw(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeText(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

void SerializationDynamic::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readStringUntilEOF(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeWholeText(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeWholeText(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Raw, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeWholeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeWholeText(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    dynamic_column.getVariantInfo().variant_type->getDefaultSerialization()->serializeTextXML(dynamic_column.getVariantColumn(), row_num, ostr, settings);
}

}
