#include "config.h"

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromVector.h>

#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

class FunctionStructureToProtobufSchema : public IFunction
{
public:
    FunctionStructureToProtobufSchema(ContextPtr context_) : context(std::move(context_))
    {
    }

    static constexpr auto name = "structureToProtobufSchema";

    static FunctionPtr create(ContextPtr ctx)
    {
        return std::make_shared<FunctionStructureToProtobufSchema>(std::move(ctx));
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const  override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1",
                getName(), arguments.size());

        if (!isString(arguments[0]))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the first argument of function {}, expected constant string",
                arguments[0]->getName(),
                getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1",
                getName(), arguments.size());

        String structure = arguments[0].column->getDataAt(0).toString();
        auto columns_list = parseColumnsListFromString(structure, context);

        auto col_res = ColumnString::create();
        auto & data = assert_cast<ColumnString &>(*col_res).getChars();
        WriteBufferFromVector buf(data);
        writeSchemaPrefix(buf);

        size_t field_index = 1;
        for (const auto & [column_name, data_type] : columns_list.getAll())
            writeProtobufField(buf, data_type, column_name, field_index, 1);

        writeSchemaSuffix(buf);
        buf.finalize();
        return ColumnConst::create(std::move(col_res), input_rows_count);
    }

private:
    void writeSchemaPrefix(WriteBuffer & buf) const
    {
        writeCString("syntax = \"proto3\";\nmessage Message {\n", buf);
    }

    void writeSchemaSuffix(WriteBuffer & buf) const
    {
        writeCString("}\n", buf);
    }

    void writeProtobufField(WriteBuffer & buf, const DataTypePtr & data_type, const String & column_name, size_t & field_index, size_t indent) const
    {
        switch (data_type->getTypeId())
        {
            case TypeIndex::Int8:
                writeFieldDefinition(buf, "int8", column_name, field_index, indent);
                return;
            case TypeIndex::UInt8:
                if (isBool(data_type))
                    writeFieldDefinition(buf, "bool", column_name, field_index, indent);
                else
                    writeFieldDefinition(buf, "uint8", column_name, field_index, indent);
                return;
            case TypeIndex::Int16:
                writeFieldDefinition(buf, "int16", column_name, field_index, indent);
                return;
            case TypeIndex::UInt16:
                writeFieldDefinition(buf, "uint16", column_name, field_index, indent);
                return;
            case TypeIndex::Int32:
                writeFieldDefinition(buf, "int32", column_name, field_index, indent);
                return;
            case TypeIndex::IPv4: [[fallthrough]];
            case TypeIndex::UInt32:
                writeFieldDefinition(buf, "uint32", column_name, field_index, indent);
                return;
            case TypeIndex::Int64:
                writeFieldDefinition(buf, "int64", column_name, field_index, indent);
                return;
            case TypeIndex::UInt64:
                writeFieldDefinition(buf, "uint64", column_name, field_index, indent);
                return;
            case TypeIndex::Int128:
                writeFieldDefinition(buf, "bytes", column_name, field_index, indent);
                return;
            case TypeIndex::UInt128:
                writeFieldDefinition(buf, "bytes", column_name, field_index, indent);
                return;
            case TypeIndex::Int256:
                writeFieldDefinition(buf, "bytes", column_name, field_index, indent);
                return;
            case TypeIndex::UInt256:
                writeFieldDefinition(buf, "bytes", column_name, field_index, indent);
                return;
            case TypeIndex::Float32:
                writeFieldDefinition(buf, "float", column_name, field_index, indent);
                return;
            case TypeIndex::Float64:
                writeFieldDefinition(buf, "double", column_name, field_index, indent);
                return;
            case TypeIndex::Decimal32: [[fallthrough]];
            case TypeIndex::Decimal64: [[fallthrough]];
            case TypeIndex::Decimal128: [[fallthrough]];
            case TypeIndex::Decimal256: [[fallthrough]];
            case TypeIndex::UUID: [[fallthrough]];
            case TypeIndex::IPv6: [[fallthrough]];
            case TypeIndex::FixedString: [[fallthrough]];
            case TypeIndex::String:
                writeFieldDefinition(buf, "bytes", column_name, field_index, indent);
                return;
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type {} is not supported in Protobuf schema", data_type->getName());
        }
    }

    void writeFieldDefinition(WriteBuffer & buf, const String & type_name, const String & column_name, size_t & field_index, size_t indent) const
    {
        writeString(fmt::format("{}{} {} = {};\n", String('\t', indent), type_name, column_name, field_index++), buf);
    }


    ContextPtr context;
};


REGISTER_FUNCTION(GenerateRandomStructure)
{
    factory.registerFunction<FunctionStructureToProtobufSchema>(
        {
            R"(

)",
            Documentation::Examples{
                {"random", "SELECT generateRandomStructure()"},
            },
            Documentation::Categories{"Other"}
        },
        FunctionFactory::CaseSensitive);
}

}
