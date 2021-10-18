#pragma once

#include <Storages/ColumnsDescription.h>
#include <Formats/FormatFactory.h>

namespace DB
{

using ReadBufferCreator = std::function<std::unique_ptr<ReadBuffer>()>;
ColumnsDescription readSchemaFromFormat(const String & format_name, const std::optional<FormatSettings> & format_settings, ReadBufferCreator read_buffer_creator, ContextPtr context);

}
