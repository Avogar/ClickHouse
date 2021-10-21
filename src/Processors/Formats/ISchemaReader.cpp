#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

Names generateDefaultColumnNames(size_t columns)
{
    Names column_names;
    for (size_t i = 0; i != columns; ++i)
        column_names.push_back("column_" + std::to_string(i));
    return column_names;
}

DataTypes generateDefaultDataTypes(size_t columns)
{

}

}
