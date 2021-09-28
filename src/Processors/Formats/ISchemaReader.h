#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class ISchemaReader
{
public:
    explicit ISchemaReader(ReadBuffer & in_);

    virtual NamesAndTypesList readSchema() = 0;

    virtual ~ISchemaReader() = default;

protected:
    ReadBuffer & in;
};


class INamesAndTypesReader : public ISchemaReader
{
public:
    explicit INamesAndTypesReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;

protected:
    virtual Names readColumnNames() = 0;
    virtual DataTypes readColumnDataTypes() = 0;
};

}
