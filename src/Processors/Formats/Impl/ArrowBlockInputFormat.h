#pragma once
#include "config_formats.h"

#if USE_ARROW

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>

namespace arrow { class RecordBatchReader; }
namespace arrow::ipc { class RecordBatchFileReader; }

namespace DB
{

class ReadBuffer;
class ArrowColumnToCHColumn;

class ArrowBlockInputFormat : public IInputFormat
{
public:
    ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_, bool stream_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ArrowBlockInputFormat"; }

protected:
    Chunk generate() override;

private:
    // Whether to use ArrowStream format
    bool stream;
    // This field is only used for ArrowStream format
    std::shared_ptr<arrow::RecordBatchReader> stream_reader;
    // The following fields are used only for Arrow format
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    int record_batch_total = 0;
    int record_batch_current = 0;

    const FormatSettings format_settings;

    void prepareReader();
};

class ArrowSchemaReader : public ISchemaReader
{
public:
    explicit ArrowSchemaReader(bool stream);

    NamesAndTypesList readSchema(ReadBuffer & in) const override;

private:
    bool stream;
};

}

#endif
