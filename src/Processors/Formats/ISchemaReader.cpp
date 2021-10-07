#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

IExternalSchemaReader::IExternalSchemaReader(const FormatSettings & format_settings_) : format_settings(format_settings_)
{
}

NamesAndTypesList INamesAndTypesReader::readSchema(ReadBuffer & in)
{
    auto column_names = readColumnNames(in);
    auto column_types = readColumnDataTypes(in);
    return NamesAndTypesList::createFromNamesAndTypes(column_names, column_types);
}

}
