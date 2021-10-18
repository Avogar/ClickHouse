#include <Formats/readSchemaFromFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

ColumnsDescription readSchemaFromFormat(const String & format_name, const std::optional<FormatSettings> & format_settings, ReadBufferCreator read_buffer_creator, ContextPtr context)
{
    NamesAndTypesList names_and_types;
    if (FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format_name))
    {
        auto external_schema_reader = FormatFactory::instance().getExternalSchemaReader(format_name, context, format_settings);
        try
        {
            names_and_types = external_schema_reader->readSchema();
        }
        catch (const DB::Exception & e)
        {
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file. Error: {}", format_name, e.message());
        }
    } else if (FormatFactory::instance().checkIfFormatHasSchemaReader(format_name))
    {
        auto read_buf = read_buffer_creator();
        if (read_buf->eof())
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file, file is empty", format_name);

        auto schema_reader = FormatFactory::instance().getSchemaReader(format_name, context, format_settings);
        try
        {
            names_and_types = schema_reader->readSchema(*read_buf);
        }
        catch (const DB::Exception & e)
        {
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot extract table structure from {} format file. Error: {}", format_name, e.message());
        }
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} file format doesn't support schema inference", format_name);

    return ColumnsDescription(names_and_types);
}

}
