#pragma once

#include <Formats/FormatSettings.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Poco/JSON/Object.h>
#include <utility>

namespace DB
{

std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size);
std::pair<bool, size_t> fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows);

DataTypePtr getDataTypeFromJSONField(const Poco::Dynamic::Var & field);

std::unordered_map<String, DataTypePtr> readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, bool json_strings);

DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, bool json_strings);

bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf);

bool readFieldImpl(ReadBuffer & in, IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, const String & column_name, const FormatSettings & format_settings, bool yield_strings);

}
