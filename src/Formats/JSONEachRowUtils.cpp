#include <IO/ReadHelpers.h>
#include <Formats/JSONEachRowUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/ReadBufferFromString.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Poco/JSON/Parser.h>

#include <base/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

template <const char opening_bracket, const char closing_bracket>
static std::pair<bool, size_t> fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
{
    skipWhitespaceIfAny(in);

    char * pos = in.position();
    size_t balance = 0;
    bool quotes = false;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && (balance || memory.size() + static_cast<size_t>(pos - in.position()) < min_chunk_size || number_of_rows < min_rows))
    {
        const auto current_object_size = memory.size() + static_cast<size_t>(pos - in.position());
        if (current_object_size > 10 * min_chunk_size)
            throw ParsingException("Size of JSON object is extremely large. Expected not greater than " +
            std::to_string(min_chunk_size) + " bytes, but current is " + std::to_string(current_object_size) +
            " bytes per row. Increase the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, most likely JSON is malformed", ErrorCodes::INCORRECT_DATA);

        if (quotes)
        {
            pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());

            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
                continue;

            if (*pos == '\\')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos))
                    ++pos;
            }
            else if (*pos == '"')
            {
                ++pos;
                quotes = false;
            }
        }
        else
        {
            pos = find_first_symbols<opening_bracket, closing_bracket, '\\', '"'>(pos, in.buffer().end());

            if (pos > in.buffer().end())
                throw Exception("Position in buffer is out of bounds. There must be a bug.", ErrorCodes::LOGICAL_ERROR);
            else if (pos == in.buffer().end())
                continue;

            else if (*pos == opening_bracket)
            {
                ++balance;
                ++pos;
            }
            else if (*pos == closing_bracket)
            {
                --balance;
                ++pos;
            }
            else if (*pos == '\\')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos))
                    ++pos;
            }
            else if (*pos == '"')
            {
                quotes = true;
                ++pos;
            }

            if (balance == 0)
                ++number_of_rows;
        }
    }

    saveUpToPosition(in, memory, pos);
    return {loadAtPosition(in, memory, pos), number_of_rows};
}

template <const char opening_bracket, const char closing_bracket>
static String readJSONEachRowLineIntoStringImpl(ReadBuffer & in)
{
    skipWhitespaceIfAny(in);

    if (in.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read JSON object: unexpected end of file");

    char * pos = in.position();
    if (*pos != opening_bracket)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read JSONEachRow line: {} expected, {} got", opening_bracket, *in.position());
    ++pos;

    Memory memory;
    size_t balance = 1;
    bool quotes = false;
    while (loadAtPosition(in, memory, pos) && balance)
    {
        if (quotes)
        {
            pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());

            if (pos == in.buffer().end())
                continue;

            if (*pos == '\\')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos))
                    ++pos;
            }
            else if (*pos == '"')
            {
                ++pos;
                quotes = false;
            }
        }
        else
        {
            pos = find_first_symbols<opening_bracket, closing_bracket, '\\', '"'>(pos, in.buffer().end());

            if (pos == in.buffer().end())
                continue;

            else if (*pos == opening_bracket)
            {
                ++balance;
                ++pos;
            }
            else if (*pos == closing_bracket)
            {
                --balance;
                ++pos;
            }
            else if (*pos == '\\')
            {
                ++pos;
                if (loadAtPosition(in, memory, pos))
                    ++pos;
            }
            else if (*pos == '"')
            {
                quotes = true;
                ++pos;
            }
        }
    }

    if (balance)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read JSON object: unexpected end of file");

    saveUpToPosition(in, memory, pos);
    return String(memory.data(), memory.size());
}

static DataTypePtr getDataTypeFromJSONField(const Poco::Dynamic::Var & field)
{
    if (field.isEmpty())
        return nullptr;

    if (field.isNumeric())
        return makeNullable(std::make_shared<DataTypeFloat64>());

    if (field.isString())
        return makeNullable(std::make_shared<DataTypeString>());

    if (field.isArray())
    {
        Poco::JSON::Array::Ptr array = field.extract<Poco::JSON::Array::Ptr>();
        if (array->size() == 0)
            return nullptr;

        DataTypes nested_data_types;
        bool is_tuple = false;
        for (size_t i = 0; i != array->size(); ++i)
        {
            auto type = getDataTypeFromJSONField(array->get(i));
            if (!type)
                return nullptr;

            if (!nested_data_types.empty() && type->getName() != nested_data_types.back()->getName())
                is_tuple = true;

            nested_data_types.push_back(std::move(type));
        }

        if (is_tuple)
            return std::make_shared<DataTypeTuple>(nested_data_types);

        return std::make_shared<DataTypeArray>(nested_data_types.back());
    }

    throw Exception{ErrorCodes::INCORRECT_DATA, "Unexpected JSON type {}", field.type().name()};
}

template <const char opening_bracket, const char closing_bracket>
static DataTypes determineColumnDataTypesFromJSONEachRowDataImpl(ReadBuffer & in, size_t max_depth, bool json_strings, const String & format_name, JSONEachRowFieldExtractor extractor)
{
    if (json_strings)
    {
        size_t number_of_columns = 0;
        skipWhitespaceIfAny(in);
        assertChar(opening_bracket, in);
        do
        {
            skipWhitespaceIfAny(in);
            skipJSONField(in, "column_" + std::to_string(number_of_columns));
            ++number_of_columns;
        }
        while (checkChar(',', in));
        skipWhitespaceIfAny(in);
        assertChar(closing_bracket, in);
        return generateDefaultDataTypes(number_of_columns);
    }

    Poco::JSON::Parser parser;
    DataTypes data_types;
    std::unordered_set<size_t> parsed_indexes;
    size_t attempts = 0;
    size_t number_of_columns = 0;
    do
    {
        if (in.eof())
            break;

        String line = readJSONEachRowLineIntoStringImpl<opening_bracket, closing_bracket>(in);
        auto var = parser.parse(line);
        std::vector<Poco::Dynamic::Var> fields = extractor(var);

        if (number_of_columns == 0)
        {
            number_of_columns = fields.size();
            data_types.resize(number_of_columns, nullptr);
        }
        else if (fields.size() != number_of_columns)
            throw Exception{ErrorCodes::INCORRECT_DATA, "{} rows have different number of columns", format_name};

        for (size_t i = 0; i != number_of_columns; ++i)
        {
            if (!parsed_indexes.contains(i) && (data_types[i] = getDataTypeFromJSONField(fields[i])))
                parsed_indexes.insert(i);
        }

        ++attempts;
    }
    while (parsed_indexes.size() != number_of_columns && attempts < max_depth);

    if (parsed_indexes.size() != number_of_columns)
    {
        throw Exception(
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "Cannot determine table structure by first {} rows of parsed {} data, because some columns contain only Nulls. To increase the maximum "
            "number of rows to read for structure determination, use setting input_format_msgpack_max_depth_for_schema_inference", max_depth, format_name);
    }

    return data_types;
}

std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
{
    return fileSegmentationEngineJSONEachRowImpl<'{', '}'>(in, memory, min_chunk_size, 1);
}

std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
{
    return fileSegmentationEngineJSONEachRowImpl<'[', ']'>(in, memory, min_chunk_size, min_rows);
}

DataTypes determineColumnDataTypesFromJSONEachRowData(ReadBuffer & in, size_t max_depth, bool json_strings, JSONEachRowFieldExtractor extractor)
{
    return determineColumnDataTypesFromJSONEachRowDataImpl<'{', '}'>(in, max_depth, json_strings, "JSONEachRow", extractor);
}

DataTypes determineColumnDataTypesFromJSONCompactEachRowData(ReadBuffer & in, size_t max_depth, bool json_strings, JSONEachRowFieldExtractor extractor)
{
    return determineColumnDataTypesFromJSONEachRowDataImpl<'[', ']'>(in, max_depth, json_strings, "JSONCompactEachRow", extractor);
}


bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf)
{
    /// For JSONEachRow we can safely skip whitespace characters
    skipWhitespaceIfAny(buf);
    return buf.eof() || *buf.position() == '[';
}

bool readFieldImpl(ReadBuffer & in, IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name, const FormatSettings & format_settings, bool yield_strings)
{
    try
    {
        bool as_nullable = format_settings.null_as_default && !type->isNullable() && !type->isLowCardinalityNullable();

        if (yield_strings)
        {
            String str;
            readJSONString(str, in);

            ReadBufferFromString buf(str);

            if (as_nullable)
                return SerializationNullable::deserializeWholeTextImpl(column, buf, format_settings, serialization);

            serialization->deserializeWholeText(column, buf, format_settings);
            return true;
        }

        if (as_nullable)
            return SerializationNullable::deserializeTextJSONImpl(column, in, format_settings, serialization);

        serialization->deserializeTextJSON(column, in, format_settings);
        return true;
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading the value of key " + column_name + ")");
        throw;
    }
}

}
