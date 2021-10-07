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
    virtual NamesAndTypesList readSchema(ReadBuffer & in) = 0;

    virtual ~ISchemaReader() = default;
};

class INamesAndTypesReader : public ISchemaReader
{
public:
    NamesAndTypesList readSchema(ReadBuffer & in) override;

protected:
    virtual Names readColumnNames(ReadBuffer & in) = 0;
    virtual DataTypes readColumnDataTypes(ReadBuffer & in) = 0;
};

class IExternalSchemaReader
{
public:
    explicit IExternalSchemaReader(const FormatSettings & format_settings_);
    virtual NamesAndTypesList readSchema() = 0;
    virtual ~IExternalSchemaReader() = default;

protected:
    const FormatSettings format_settings;
};

}
