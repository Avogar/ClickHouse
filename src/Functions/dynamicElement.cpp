#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnDynamic.h>
#include <Common/assert_cast.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Extract element of Dynamic by type name.
  * Also the function looks through Arrays: you can get Array of Dynamic elements from Array of Dynamic.
  */
class FunctionDynamicElement : public IFunction
{
public:
    static constexpr auto name = "dynamicElement";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDynamicElement>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2",
                            getName(), number_of_arguments);

        size_t count_arrays = 0;
        const IDataType * input_type = arguments[0].type.get();
        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(input_type))
        {
            input_type = array->getNestedType().get();
            ++count_arrays;
        }

        if (!isDynamic(*input_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Variant or Array of Variant. Actual {}",
                            getName(),
                            arguments[0].type->getName());

        auto return_type = makeNullableOrLowCardinalityNullableSafe(getRequestedElementType(arguments[1].column));

        for (; count_arrays; --count_arrays)
            return_type = std::make_shared<DataTypeArray>(return_type);

        return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & input_arg = arguments[0];
        const IDataType * input_type = input_arg.type.get();
        const IColumn * input_col = input_arg.column.get();

        bool input_arg_is_const = false;
        if (typeid_cast<const ColumnConst *>(input_col))
        {
            input_col = assert_cast<const ColumnConst *>(input_col)->getDataColumnPtr().get();
            input_arg_is_const = true;
        }

        Columns array_offsets;
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(input_type))
        {
            const ColumnArray * array_col = assert_cast<const ColumnArray *>(input_col);

            input_type = array_type->getNestedType().get();
            input_col = &array_col->getData();
            array_offsets.push_back(array_col->getOffsetsPtr());
        }

        const ColumnDynamic * input_col_as_dynamic = checkAndGetColumn<ColumnDynamic>(input_col);
        if (!input_col_as_dynamic)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Dynamic or array of Dynamics. Actual {}", getName(), input_arg.type->getName());

        auto element_type = getRequestedElementType(arguments[1].column);
        const auto & variant_info = input_col_as_dynamic->getVariantInfo();
        auto it = variant_info.variant_name_to_discriminator.find(element_type->getName());
        if (it == variant_info.variant_name_to_discriminator.end())
        {
            auto result_type = makeNullableOrLowCardinalityNullableSafe(element_type);
            auto result_column = result_type->createColumn();
            result_column->insertManyDefaults(input_rows_count);
            return wrapInArraysAndConstIfNeeded(std::move(result_column), array_offsets, input_arg_is_const, input_rows_count);
        }

        const auto & variant_column = input_col_as_dynamic->getVariantColumn();
        auto subcolumn_creator = SerializationVariantElement::VariantSubcolumnCreator(variant_column.getLocalDiscriminatorsPtr(), element_type->getName(), it->second, variant_column.localDiscriminatorByGlobal(it->second));
        auto result_column = subcolumn_creator.create(variant_column.getVariantPtrByGlobalDiscriminator(it->second));
        return wrapInArraysAndConstIfNeeded(std::move(result_column), array_offsets, input_arg_is_const, input_rows_count);
    }

private:
    DataTypePtr getRequestedElementType(const ColumnPtr & type_name_column) const
    {
        const auto * name_col = checkAndGetColumnConst<ColumnString>(type_name_column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of {} must be a constant String", getName());

        String element_type_name = name_col->getValue<String>();
        auto element_type = DataTypeFactory::instance().tryGet(element_type_name);
        if (!element_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of {} must be a valid type name. Got: {}", getName(), element_type_name);

        return element_type;
    }

    ColumnPtr wrapInArraysAndConstIfNeeded(ColumnPtr res, const Columns & array_offsets, bool input_arg_is_const, size_t input_rows_count) const
    {
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        if (input_arg_is_const)
            res = ColumnConst::create(res, input_rows_count);

        return res;
    }
};

}

REGISTER_FUNCTION(DynamicElement)
{
//    factory.registerFunction<FunctionDynamicElement>(FunctionDocumentation{
//        .description = R"(
//Extracts a column with specified type from a `Dynamic` column.
//)",
//        .syntax{"dynamicElement(dynamic, type_name)"},
//        .arguments{{
//            {"dynamic", "Dynamic column"},
//            {"type_name", "The name of the variant type to extract"}}},
//        .examples{{{
//            "Example",
//            R"(
//)",
//            R"(
//)"}}},
//        .categories{"Dynamic"},
//    });

    factory.registerFunction<FunctionDynamicElement>();
}

}
