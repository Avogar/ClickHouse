#include <Processors/Formats/ISchemaReader.h>

namespace DB
{
ISchemaReader::ISchemaReader(ReadBuffer & in_) : in(in_)
{
}

INamesAndTypesReader::INamesAndTypesReader(ReadBuffer & in_) : ISchemaReader(in_)
{
}

NamesAndTypesList INamesAndTypesReader::readSchema()
{
    auto column_names = readColumnNames();
    auto column_types = readColumnDataTypes();
    std::vector<NameAndTypePair> name_and_types;
    for (size_t i = 0; i != column_types.size(); ++i)
        name_and_types.emplace_back(column_names[i], column_types[i]);
    return NamesAndTypesList(name_and_types.begin(), name_and_types.end());
}

}
