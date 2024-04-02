#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

class DataTypeDynamic final : public IDataType
{
public:
    static constexpr bool is_parametric = false;

    DataTypeDynamic() = default;

    TypeIndex getTypeId() const override { return TypeIndex::Dynamic; }
    const char * getFamilyName() const override { return "Dynamic"; }

    bool isParametric() const override { return false; }
    bool canBeInsideNullable() const override { return false; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    bool haveSubtypes() const override { return false; }

private:
    bool hasDynamicSubcolumnsData() const override { return true; }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const override;

    SerializationPtr doGetDefaultSerialization() const override;
};

}

