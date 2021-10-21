#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageFile.h>
#include <Storages/Distributed/DirectoryMonitor.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Processors/ISource.h>

#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void checkIfFormatSupportsAutoStructure(const String & name, const String & format)
    {
        if (name == "file" && format == "Distributed")
            return;

        if (FormatFactory::instance().checkIfFormatHasAnySchemaReader(format))
            return;

        throw Exception(
            "Table function '" + name
                + "' allows automatic structure determination only for formats that support schema inference and for Distributed format in table function "
                  "'file'",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

void ITableFunctionFileLike::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 2)
        throw Exception("Table function '" + getName() + "' requires at least 2 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    format = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() == 2)
    {
        checkIfFormatSupportsAutoStructure(getName(), format);
        return;
    }

    if (args.size() != 3 && args.size() != 4)
        throw Exception("Table function '" + getName() + "' requires 3 or 4 arguments: filename, format, structure and compression method (default auto)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    if (structure == "auto")
        checkIfFormatSupportsAutoStructure(getName(), format);

    if (structure.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table structure is empty for table function '{}'",
            ast_function->formatForErrorMessage());

    if (args.size() == 4)
        compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = parseStructure(context);
    StoragePtr storage = getStorage(filename, format, columns, context, table_name, compression_method);
    storage->startup();
    return storage;
}

ColumnsDescription ITableFunctionFileLike::getActualTableStructure(ContextPtr context) const
{
    if (!hasStaticStructure())
        throw Exception{ErrorCodes::LOGICAL_ERROR, "Cannot get actual table structure"};

    return parseStructure(context);
}

ColumnsDescription ITableFunctionFileLike::parseStructure(ContextPtr context) const
{
    if (structure == "auto")
    {
        if (getName() == "file" && format == "Distributed")
        {
            size_t total_bytes_to_read = 0;
            Strings paths = StorageFile::getPathsList(filename, context->getUserFilesPath(), context, total_bytes_to_read);
            if (paths.empty())
                throw Exception(
                    "Cannot get table structure from file, because no files match specified name", ErrorCodes::INCORRECT_FILE_NAME);
            auto source = StorageDistributedDirectoryMonitor::createSourceFromFile(paths[0]);
            return ColumnsDescription(source->getOutputs().front().getHeader().getNamesAndTypesList());
        }
        /// Return an empty list of columns - the structure will be determined in Storage constructor.
        return ColumnsDescription();
    }

    return parseColumnsListFromString(structure, context);
}

}
