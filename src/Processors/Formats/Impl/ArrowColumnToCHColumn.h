#pragma once
#include "config_formats.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <arrow/type.h>
#include <Columns/ColumnVector.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/buffer.h>
#include <Processors/Chunk.h>
#include <Core/Block.h>

namespace DB
{

class ArrowColumnToCHColumn
{
public:
    ArrowColumnToCHColumn(const Block & header_, const std::string & format_name_, bool import_nested_);

    /// Create header by arrow schema. It will be useful for inserting
    /// data from file without knowing table structure.
    ArrowColumnToCHColumn(const arrow::Schema & schema, const std::string & format_name, bool import_nested_);

    void arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table);

private:
#define FOR_ARROW_NUMERIC_TYPES(M) \
        M(arrow::Type::UINT8, DB::UInt8) \
        M(arrow::Type::INT8, DB::Int8) \
        M(arrow::Type::UINT16, DB::UInt16) \
        M(arrow::Type::INT16, DB::Int16) \
        M(arrow::Type::UINT32, DB::UInt32) \
        M(arrow::Type::INT32, DB::Int32) \
        M(arrow::Type::UINT64, DB::UInt64) \
        M(arrow::Type::INT64, DB::Int64) \
        M(arrow::Type::HALF_FLOAT, DB::Float32) \
        M(arrow::Type::FLOAT, DB::Float32) \
        M(arrow::Type::DOUBLE, DB::Float64)

#define FOR_ARROW_INDEXES_TYPES(M) \
        M(arrow::Type::UINT8, DB::UInt8) \
        M(arrow::Type::INT8, DB::UInt8) \
        M(arrow::Type::UINT16, DB::UInt16) \
        M(arrow::Type::INT16, DB::UInt16) \
        M(arrow::Type::UINT32, DB::UInt32) \
        M(arrow::Type::INT32, DB::UInt32) \
        M(arrow::Type::UINT64, DB::UInt64) \
        M(arrow::Type::INT64, DB::UInt64)


    const Block header;
    const std::string format_name;
    bool import_nested;
    /// Map {column name : dictionary column}.
    /// To avoid converting dictionary from Arrow Dictionary
    /// to LowCardinality every chunk we save it and reuse.
    std::unordered_map<std::string, std::shared_ptr<ColumnWithTypeAndName>> dictionary_values;
};
}
#endif
