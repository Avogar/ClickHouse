#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerWithNamesAndTypes.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SKIP_UNKNOWN_FIELD;
}

BinaryRowInputFormat::BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_, bool with_types_, const FormatSettings & format_settings_)
    : RowInputFormatWithNamesAndTypes(std::move(header), in_, std::move(params_), with_names_, with_types_, format_settings_)
{
}

std::vector<String> BinaryRowInputFormat::readHeaderRow()
{
    std::vector<String> fields;
    String field;
    for (size_t i = 0; i < read_columns; ++i)
    {
        readStringBinary(field, *in);
        fields.push_back(field);
    }
    return fields;
}

std::vector<String> BinaryRowInputFormat::readNames()
{
    readVarUInt(read_columns, *in);
    return readHeaderRow();
}

std::vector<String> BinaryRowInputFormat::readTypes()
{
    auto types = readHeaderRow();
    for (const auto & type_name : types)
        read_data_types.push_back(DataTypeFactory::instance().get(type_name));
    return types;
}

bool BinaryRowInputFormat::readField(IColumn & column, const DataTypePtr & /*type*/, const SerializationPtr & serialization, bool /*is_last_file_column*/, const String & /*column_name*/)
{
    serialization->deserializeBinary(column, *in);
    return true;
}

void BinaryRowInputFormat::skipHeaderRow()
{
    String tmp;
    for (size_t i = 0; i < read_columns; ++i)
        readStringBinary(tmp, *in);
}

void BinaryRowInputFormat::skipNames()
{
    readVarUInt(read_columns, *in);
    skipHeaderRow();
}

void BinaryRowInputFormat::skipTypes()
{
    skipHeaderRow();
}

void BinaryRowInputFormat::skipField(size_t file_column)
{
    if (file_column >= read_data_types.size())
        throw Exception(ErrorCodes::CANNOT_SKIP_UNKNOWN_FIELD, "Cannot skip unknown field in RowBinaryWithNames format, because it's type is unknown");
    Field field;
    read_data_types[file_column]->getDefaultSerialization()->deserializeBinary(field, *in);
}

Names BinaryWithNamesAndTypesSchemaReader::readColumnNames(ReadBuffer & in, UInt64 columns) const
{
    std::vector<String> column_names;
    String column_name;
    for (size_t i = 0; i < columns; ++i)
    {
        readStringBinary(column_name, in);
        column_names.push_back(column_name);
    }
    return column_names;
}

DataTypes BinaryWithNamesAndTypesSchemaReader::readColumnDataTypes(ReadBuffer & in, UInt64 columns) const
{
    std::vector<String> type_names = readColumnNames(in, columns);
    std::vector<DataTypePtr> data_types;
    for (const auto & type_name : type_names)
        data_types.push_back(DataTypeFactory::instance().get(type_name));
    return data_types;
}

NamesAndTypesList BinaryWithNamesAndTypesSchemaReader::readSchema(ReadBuffer & in) const
{
    UInt64 columns;
    readVarUInt(columns, in);
    auto column_names = readColumnNames(in, columns);
    auto column_types = readColumnDataTypes(in, columns);
    return NamesAndTypesList::createFromNamesAndTypes(column_names, column_types);
}

void registerInputFormatRowBinary(FormatFactory & factory)
{
    auto register_func = [&](const String & format_name, bool with_names, bool with_types)
    {
        factory.registerInputFormat(format_name, [with_names, with_types](
            ReadBuffer & buf,
            const Block & sample,
            const IRowInputFormat::Params & params,
            const FormatSettings & settings)
        {
            return std::make_shared<BinaryRowInputFormat>(buf, sample, params, with_names, with_types, settings);
        });
    };

    registerWithNamesAndTypes("RowBinary", register_func);
}

void registerRowBinaryWithNamesAndTypesSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("RowBinaryWithNamesAndTypes", [](const FormatSettings &)
    {
        return std::make_shared<BinaryWithNamesAndTypesSchemaReader>();
    });
}


}
