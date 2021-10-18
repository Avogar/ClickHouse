#pragma once
#include "config_formats.h"
#if USE_ORC

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

#include <arrow/adapters/orc/adapter.h>

namespace arrow::adapters::orc
{
    class ORCFileReader;
}

namespace DB
{

class ArrowColumnToCHColumn;

class ORCBlockInputFormat : public IInputFormat
{
public:
    ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "ORCBlockInputFormat"; }

    void resetParser() override;

protected:
    Chunk generate() override;

private:

    // TODO: check that this class implements every part of its parent

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    std::vector<String> column_names;

    int stripe_total = 0;

    int stripe_current = 0;

    // indices of columns to read from ORC file
    std::vector<int> include_indices;

    const FormatSettings format_settings;

    void prepareReader();
};

class ORCSchemaReader : public ISchemaReader
{
public:
    NamesAndTypesList readSchema(ReadBuffer & in) override;
};

}
#endif
