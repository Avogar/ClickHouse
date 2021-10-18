#pragma once

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ReadBuffer;


/** A stream for reading data in a bunch of formats:
 *  - JSONCompactEachRow
 *  - JSONCompactEachRowWithNamesAndTypes
 *  - JSONCompactStringsEachRow
 *  - JSONCompactStringsEachRowWithNamesAndTypes
 *
*/
class JSONCompactEachRowRowInputFormat : public RowInputFormatWithNamesAndTypes
{
public:
    JSONCompactEachRowRowInputFormat(
        const Block & header_,
        ReadBuffer & in_,
        Params params_,
        bool with_names_,
        bool with_types_,
        bool yield_strings_,
        const FormatSettings & format_settings_);

    String getName() const override { return "JSONCompactEachRowRowInputFormat"; }

private:
    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    bool parseRowStartWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer & out) override;
    bool parseRowEndWithDiagnosticInfo(WriteBuffer & out) override;
    bool isGarbageAfterField(size_t, ReadBuffer::Position pos) override
    {
        return *pos != ',' && *pos != ']' && *pos != ' ' && *pos != '\t';
    }

    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

    void skipField(size_t file_column) override;
    void skipHeaderRow();
    void skipNames() override { skipHeaderRow(); }
    void skipTypes() override { skipHeaderRow(); }
    void skipRowStartDelimiter() override;
    void skipFieldDelimiter() override;
    void skipRowEndDelimiter() override;

    bool yield_strings;
};

class JSONCompactEachRowRowSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    JSONCompactEachRowRowSchemaReader(bool with_names_, bool with_types_, bool yield_strings_, const FormatSettings & format_settings_);

    Names readColumnNames(ReadBuffer & in) override;
    Names readDataTypeNames(ReadBuffer & in) override;

private:
    DataTypes determineTypesFromData(ReadBuffer & in) override;

    std::vector<std::string> readRow(ReadBuffer & in);

    bool json_strings;
    size_t max_depth_for_schema_inference;
};

}
