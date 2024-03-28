#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnDynamic.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

MutableColumnPtr DataTypeDynamic::createColumn() const
{
    return ColumnDynamic::create();
}

Field DataTypeDynamic::getDefault() const
{
    return Field(Null());
}

SerializationPtr DataTypeDynamic::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDynamic>();
}

void registerDataTypeDynamic(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("Dynamic", [] { return DataTypePtr(std::make_shared<DataTypeDynamic>()); });
}

bool DataTypeDynamic::hasDynamicSubcolumn(std::string_view subcolumn_name) const
{
    auto [type_name, type_subcolumn_name] = Nested::splitName(subcolumn_name);
    auto type = DataTypeFactory::instance().tryGet(String(type_name));
    if (!type)
        return false;

    return type_subcolumn_name.empty() || type->hasSubcolumn(subcolumn_name);
}

std::unique_ptr<IDataType::SubstreamData> DataTypeDynamic::getDynamicSubcolumnData(std::string_view subcolumn_name, const DB::IDataType::SubstreamData & data, bool throw_if_null) const
{
    auto [subcolumn_type_name, subcolumn_nested_name] = Nested::splitName(subcolumn_name);
    auto subcolumn_type = DataTypeFactory::instance().tryGet(String(subcolumn_type_name));
    if (!subcolumn_type)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dynamic type doesn't have subcolumn '{}'", subcolumn_type_name);
        return nullptr;
    }

    auto subcolumn_result_type = makeNullableOrLowCardinalityNullableSafe(subcolumn_type);
    std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(subcolumn_type->getDefaultSerialization());
    res->withType(subcolumn_result_type);
    if (data.column)
    {
        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*data.column);
        const auto & variant_info = dynamic_column.getVariantInfo();
        auto it = variant_info.variant_name_to_discriminator.find(subcolumn_type->getName());
        if (it != variant_info.variant_name_to_discriminator.end())
        {
            auto subcolumn = variant_info.variant_type->getSubcolumn(subcolumn_type->getName(), dynamic_column.getVariantColumnPtr());
            res->withColumn(std::move(subcolumn));
        }
        else
        {
            auto subcolumn = subcolumn_result_type->createColumn();
            subcolumn->insertManyDefaults(dynamic_column.size());
            res->withColumn(std::move(subcolumn));
        }
    }

    if (!subcolumn_nested_name.empty())
        res = getSubcolumnData(subcolumn_nested_name, *res, throw_if_null);

    /// res->serialization = ...
    return res;
}

}
