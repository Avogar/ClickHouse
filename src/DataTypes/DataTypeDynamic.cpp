#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
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

std::unique_ptr<IDataType::SubstreamData> DataTypeDynamic::getDynamicSubcolumnData(std::string_view subcolumn_name, const DB::IDataType::SubstreamData & data, bool throw_if_null) const
{
//    std::cerr << "Get subcolumn data for subcolumn " << subcolumn_name << " from Dynamic type\n";
    auto [subcolumn_type_name, subcolumn_nested_name] = Nested::splitName(subcolumn_name);
    auto subcolumn_type = DataTypeFactory::instance().tryGet(String(subcolumn_type_name));
    if (!subcolumn_type)
    {
//        std::cerr << "No such type\n";
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dynamic type doesn't have subcolumn '{}'", subcolumn_type_name);
        return nullptr;
    }

    std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(subcolumn_type->getDefaultSerialization());
    res->type = subcolumn_type;
    std::optional<ColumnVariant::Discriminator> discriminator;
    if (data.column)
    {
//        std::cerr << "Column provided\n";
        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*data.column);
        const auto & variant_info = dynamic_column.getVariantInfo();
//        std::cerr << "Variant: " << variant_info.variant_type->getName() << "\n";
        auto it = variant_info.variant_name_to_discriminator.find(subcolumn_type->getName());
        if (it != variant_info.variant_name_to_discriminator.end())
        {
            discriminator = it->second;
            res->column = dynamic_column.getVariantColumn().getVariantPtrByGlobalDiscriminator(*discriminator);
        }
    }

    if (!subcolumn_nested_name.empty())
    {
        res = getSubcolumnData(subcolumn_nested_name, *res, throw_if_null);
        if (!res)
            return nullptr;
    }

    res->serialization = std::make_shared<SerializationDynamicElement>(res->serialization, subcolumn_type->getName());
    res->type = makeNullableOrLowCardinalityNullableSafe(res->type);
    if (data.column)
    {
        if (discriminator)
        {
//            std::cerr << "Have subcolumn\n";
            const auto & variant_column = assert_cast<const ColumnDynamic &>(*data.column).getVariantColumn();
            auto creator = SerializationVariantElement::VariantSubcolumnCreator(variant_column.getLocalDiscriminatorsPtr(), "", *discriminator, variant_column.localDiscriminatorByGlobal(*discriminator));
            res->column = creator.create(res->column);
        }
        else
        {
//            std::cerr << "Doesn't have subcolumn\n";
            auto column = res->type->createColumn();
            column->insertManyDefaults(data.column->size());
            res->column = std::move(column);
        }
    }

//    std::cerr << "Result type: " << res->type->getName() << "\n";
    return res;
}

}
