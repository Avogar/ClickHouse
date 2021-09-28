#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>


namespace DB
{

class ReadBuffer;


/** A stream for inputting data in a binary line-by-line format.
  */
class BinaryRowInputFormat : public IRowInputFormat
{
public:
    BinaryRowInputFormat(ReadBuffer & in_, Block header, Params params_, bool with_names_, bool with_types_);

    bool readRow(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;

    String getName() const override { return "BinaryRowInputFormat"; }

private:
    bool with_names;
    bool with_types;
};

class BinaryWithNamesAndTypesSchemaReader : public INamesAndTypesReader
{
public:
    BinaryWithNamesAndTypesSchemaReader(ReadBuffer & in_);

    Names readColumnNames() override;

    DataTypes readColumnDataTypes() override;

private:
    UInt64 columns;
};

}
