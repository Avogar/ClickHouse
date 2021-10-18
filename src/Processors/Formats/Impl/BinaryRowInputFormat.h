#pragma once

#include <Core/Block.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/Formats/ISchemaReader.h>


namespace DB
{

class ReadBuffer;


/** A stream for inputting data in a binary line-by-line format.
  */
class BinaryRowInputFormat : public RowInputFormatWithNamesAndTypes
{
public:
    BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    String getName() const override { return "BinaryRowInputFormat"; }

    /// RowInputFormatWithNamesAndTypes implements logic with DiagnosticInfo, but
    /// in this format we cannot provide any DiagnosticInfo, because here we have
    /// just binary data.
    std::string getDiagnosticInfo() override { return {}; }

private:
    bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;
    void skipField(size_t file_column) override;

    void skipNames() override;
    void skipTypes() override;
    void skipHeaderRow();

    /// Data types read from input data.
    DataTypes read_data_types;
    UInt64 read_columns;
};

class BinaryWithNamesAndTypesSchemaReader : public FormatWithNamesAndTypesSchemaReader
{
public:
    BinaryWithNamesAndTypesSchemaReader();

    Names readColumnNames(ReadBuffer & in) override;
    Names readDataTypeNames(ReadBuffer & in) override;

private:
    DataTypes determineTypesFromData(ReadBuffer &) override
    {
        throw Exception{ErrorCodes::NOT_IMPLEMENTED, "Method determineTypesFromData is not implemented in BinaryWithNamesAndTypesSchemaReader"};
    }

    std::vector<std::string> readRow(ReadBuffer & in);

    UInt64 read_columns;
};

}
