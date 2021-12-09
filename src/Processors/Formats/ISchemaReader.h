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
    ISchemaReader(ReadBuffer & in_) : in(in_) {}

    virtual NamesAndTypesList readSchema() = 0;

    virtual ~ISchemaReader() = default;

protected:
    ReadBuffer & in;
};

class IRowSchemaReader : public ISchemaReader
{
public:
    IRowSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_ = nullptr);
    NamesAndTypesList readSchema() override;

protected:
    virtual DataTypes readRowAndGetDataTypes() = 0;

    void setColumnNames(const std::vector<String> & names) { column_names = names; }

private:
    size_t max_rows_to_read;
    DataTypePtr default_type;
    std::vector<String> column_names;
};

class IRowWithNamesSchemaReader : public ISchemaReader
{
public:
    IRowWithNamesSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_ = nullptr);
    NamesAndTypesList readSchema() override;

protected:
    virtual std::unordered_map<String, DataTypePtr> readRowAndGetNamesAndDataTypes() = 0;

private:
    size_t max_rows_to_read;
    DataTypePtr default_type;
};

class IExternalSchemaReader
{
public:
    virtual NamesAndTypesList readSchema() = 0;

    virtual ~IExternalSchemaReader() = default;
};

}
