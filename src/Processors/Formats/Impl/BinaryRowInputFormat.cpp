#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

BinaryRowInputFormat::BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_, bool with_types_)
    : IRowInputFormat(std::move(header), in_, params_), with_names(with_names_), with_types(with_types_)
{
}


bool BinaryRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
        serializations[i]->deserializeBinary(*columns[i], *in);

    return true;
}


void BinaryRowInputFormat::readPrefix()
{
    /// NOTE: The header is completely ignored. This can be easily improved.

    UInt64 columns = 0;
    String tmp;

    if (with_names || with_types)
    {
        readVarUInt(columns, *in);
    }

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            readStringBinary(tmp, *in);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            readStringBinary(tmp, *in);
        }
    }
}

BinaryWithNamesAndTypesSchemaReader::BinaryWithNamesAndTypesSchemaReader(ReadBuffer & in_) : INamesAndTypesReader(in_)
{
    readVarUInt(columns, in);
}

Names BinaryWithNamesAndTypesSchemaReader::readColumnNames()
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

DataTypes BinaryWithNamesAndTypesSchemaReader::readColumnDataTypes()
{
    std::vector<String> type_names = readColumnNames();
    std::vector<DataTypePtr> data_types;
    for (const auto & type_name : type_names)
        data_types.push_back(DataTypeFactory::instance().get(type_name));
    return data_types;
}

void registerInputFormatProcessorRowBinary(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("RowBinary", [](
        ReadBuffer & buf,
        const Block & sample,
        const IRowInputFormat::Params & params,
        const FormatSettings &)
    {
        return std::make_shared<BinaryRowInputFormat>(buf, sample, params, false, false);
    });

    factory.registerInputFormatProcessor("RowBinaryWithNamesAndTypes", [](
        ReadBuffer & buf,
        const Block & sample,
        const IRowInputFormat::Params & params,
        const FormatSettings &)
    {
        return std::make_shared<BinaryRowInputFormat>(buf, sample, params, true, true);
    });
}

void registerRowBinaryWithNamesAndTypesSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader("RowBinaryWithNamesAndTypes", [](ReadBuffer & in, const FormatSettings &)
    {
        return std::make_shared<BinaryWithNamesAndTypesSchemaReader>(in);
    });
}


}
