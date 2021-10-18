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
    virtual NamesAndTypesList readSchema(ReadBuffer & in) const = 0;

    virtual ~ISchemaReader() = default;
};

class IExternalSchemaReader
{
public:
    virtual NamesAndTypesList readSchema() const = 0;

    virtual ~IExternalSchemaReader() = default;
};

Names generateDefaultColumnNames(size_t columns);

}
