#include <Columns/ColumnDynamic.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/FieldToDataType.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Interpreters/castColumn.h>

namespace DB
{

ColumnDynamic::ColumnDynamic()
{
    variant_info.variant_type = std::make_shared<DataTypeVariant>(DataTypes{});
    variant_column = variant_info.variant_type->createColumn();
}

ColumnDynamic::ColumnDynamic(MutableColumnPtr variant_column_, const VariantInfo & variant_info_) : variant_column(std::move(variant_column_)), variant_info(variant_info_)
{
}

ColumnDynamic::MutablePtr ColumnDynamic::create(MutableColumnPtr variant_column, const DataTypePtr & variant_type)
{
    VariantInfo variant_info;
    variant_info.variant_type = variant_type;
    const auto & variants = assert_cast<const DataTypeVariant &>(*variant_type).getVariants();
    variant_info.variant_names.reserve(variants.size());
    variant_info.variant_name_to_discriminator.reserve(variants.size());
    for (ColumnVariant::Discriminator discr = 0; discr != variants.size(); ++discr)
    {
        variant_info.variant_names.push_back(variants[discr]->getName());
        variant_info.variant_name_to_discriminator[variant_info.variant_names.back()] = discr;
    }

    return create(std::move(variant_column), variant_info);
}

bool ColumnDynamic::addNewVariant(const DB::DataTypePtr & new_variant)
{
    /// Check if we already have such variant.
    if (variant_info.variant_name_to_discriminator.contains(new_variant->getName()))
        return true;

    /// Check if we reached maximum number of variants.
    if (variant_info.variant_names.size() == ColumnVariant::MAX_NESTED_COLUMNS)
    {
        /// ColumnDynamic can have MAX_NESTED_COLUMNS number of variants only when it has String as a variant.
        /// Otherwise we won't be able to add cast new variants to Strings.
        if (!variant_info.variant_name_to_discriminator.contains("String"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Maximum number of variants reached, but no String variant exists");

        return false;
    }

    /// If we have MAX_NESTED_COLUMNS - 1 number of variants and don't have String variant, we can add only String variant.
    if (variant_info.variant_names.size() == ColumnVariant::MAX_NESTED_COLUMNS - 1 && new_variant->getName() != "String" && !variant_info.variant_name_to_discriminator.contains("String"))
        return false;

    const DataTypes & current_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    DataTypes all_variants = current_variants;
    all_variants.push_back(new_variant);
    auto new_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    const auto & new_variants = assert_cast<const DataTypeVariant &>(*new_variant_type).getVariants();

    std::vector<ColumnVariant::Discriminator> current_to_new_discriminators;
    current_to_new_discriminators.resize(variant_info.variant_names.size());
    Names new_variant_names;
    new_variant_names.reserve(new_variants.size());
    std::unordered_map<String, ColumnVariant::Discriminator> new_variant_name_to_discriminator;
    new_variant_name_to_discriminator.reserve(new_variants.size());
    std::vector<std::pair<MutableColumnPtr, ColumnVariant::Discriminator>> new_variant_columns_and_discriminators_to_add;
    new_variant_columns_and_discriminators_to_add.reserve(new_variants.size() - current_variants.size());

    for (ColumnVariant::Discriminator discr = 0; discr != new_variants.size(); ++discr)
    {
        String name = new_variants[discr]->getName();
        new_variant_names.push_back(name);
        new_variant_name_to_discriminator[name] = discr;
        auto it = variant_info.variant_name_to_discriminator.find(name);
        if (it == variant_info.variant_name_to_discriminator.end())
            new_variant_columns_and_discriminators_to_add.emplace_back(new_variants[discr]->createColumn(), discr);
        else
            current_to_new_discriminators[it->second] = discr;
    }

    variant_info.variant_type = new_variant_type;
    variant_info.variant_names = new_variant_names;
    variant_info.variant_name_to_discriminator = new_variant_name_to_discriminator;
    assert_cast<ColumnVariant &>(*variant_column).extend(current_to_new_discriminators, std::move(new_variant_columns_and_discriminators_to_add));
    return true;
}

void ColumnDynamic::addStringVariant()
{
    addNewVariant(std::make_shared<DataTypeString>());
}

std::optional<std::vector<ColumnVariant::Discriminator>> ColumnDynamic::combineVariants(const DB::ColumnDynamic::VariantInfo & other_variant_info)
{
    const DataTypes & current_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    const DataTypes & other_variants = assert_cast<const DataTypeVariant &>(*other_variant_info.variant_type).getVariants();
    DataTypes all_variants = current_variants;
    for (size_t i = 0; i != other_variants.size(); ++i)
    {
        if (!variant_info.variant_name_to_discriminator.contains(other_variant_info.variant_names[i]))
            all_variants.push_back(other_variants[i]);
    }

    /// We cannot combine Variants if total number of variants exceeds MAX_NESTED_COLUMNS.
    if (all_variants.size() > ColumnVariant::MAX_NESTED_COLUMNS)
        return std::nullopt;

    /// We cannot combine Variants if total number of variants reaches MAX_NESTED_COLUMNS and we don't have String variant.
    if (all_variants.size() == ColumnVariant::MAX_NESTED_COLUMNS && !variant_info.variant_name_to_discriminator.contains("String") && !other_variant_info.variant_name_to_discriminator.contains("String"))
        return std::nullopt;

    auto new_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    const DataTypes & new_variants = assert_cast<const DataTypeVariant *>(new_variant_type.get())->getVariants();

    std::vector<ColumnVariant::Discriminator> other_to_new_discriminators;
    other_to_new_discriminators.resize(other_variants.size());

    /// Check if there are no new variants.
    /// In this case we should just create a global discriminators mapping for other variant.
    if (new_variants.size() == current_variants.size())
    {
        for (ColumnVariant::Discriminator discr = 0; discr != current_variants.size(); ++discr)
        {
            auto it = other_variant_info.variant_name_to_discriminator.find(variant_info.variant_names[discr]);
            if (it != other_variant_info.variant_name_to_discriminator.end())
                other_to_new_discriminators[it->second] = discr;
        }
    }
    /// We have new variants, update current variant info, extend Variant column
    /// and create a global discriminators mapping for other variant.
    else
    {
        Names new_variant_names;
        new_variant_names.reserve(new_variants.size());
        std::unordered_map<String, ColumnVariant::Discriminator> new_variant_name_to_discriminator;
        new_variant_name_to_discriminator.reserve(new_variants.size());
        std::vector<std::pair<MutableColumnPtr, ColumnVariant::Discriminator>> new_variant_columns_and_discriminators_to_add;
        new_variant_columns_and_discriminators_to_add.reserve(new_variants.size() - current_variants.size());
        std::vector<ColumnVariant::Discriminator> current_to_new_discriminators;
        current_to_new_discriminators.resize(current_variants.size());

        for (ColumnVariant::Discriminator discr = 0; discr != new_variants.size(); ++discr)
        {
            String name = new_variants[discr]->getName();
            new_variant_names.push_back(name);
            new_variant_name_to_discriminator[name] = discr;

            auto current_it = variant_info.variant_name_to_discriminator.find(name);
            if (current_it == variant_info.variant_name_to_discriminator.end())
                new_variant_columns_and_discriminators_to_add.emplace_back(new_variants[discr]->createColumn(), discr);
            else
                current_to_new_discriminators[current_it->second] = discr;

            auto other_it = other_variant_info.variant_name_to_discriminator.find(name);
            if (other_it != other_variant_info.variant_name_to_discriminator.end())
                other_to_new_discriminators[other_it->second] = discr;
        }

        variant_info.variant_type = new_variant_type;
        variant_info.variant_names = new_variant_names;
        variant_info.variant_name_to_discriminator = new_variant_name_to_discriminator;
        assert_cast<ColumnVariant &>(*variant_column).extend(current_to_new_discriminators, std::move(new_variant_columns_and_discriminators_to_add));
    }

    return other_to_new_discriminators;
}

void ColumnDynamic::insert(const DB::Field & x)
{
    /// Check if we can insert field without Variant extension.
    if (variant_column->tryInsert(x))
        return;

    /// If we cannot insert field into current variant column, extend it with new variant for this field from its type.
    if (likely(addNewVariant(applyVisitor(FieldToDataType(), x))))
    {
        /// Now we should be able to insert this field into extended variant column.
        variant_column->insert(x);
    }
    else
    {
        /// We reached maximum number of variants and couldn't add new variant.
        /// This case should be really rare in real use cases.
        /// We should always be able to add String variant and cast inserted value to String.
        addStringVariant();
        variant_column->insert(toString(x));
    }
}

bool ColumnDynamic::tryInsert(const DB::Field & x)
{
    /// We can insert any value into Dynamic column.
    insert(x);
    return true;
}


void ColumnDynamic::insertFrom(const DB::IColumn & src_, size_t n)
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_column->insertFrom(*dynamic_src.variant_column, n);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);

    /// If variants are different, we need to extend our variant with new variants.
    if (auto global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertFrom(*dynamic_src.variant_column, n, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// This case should be really rare in real use cases.
    /// We need to insert single value, try to add only corresponding variant.
    const auto & src_variant_col = assert_cast<const ColumnVariant &>(*dynamic_src.variant_column);
    auto src_global_discr = src_variant_col.globalDiscriminatorAt(n);

    /// NULL doesn't require Variant extension.
    if (src_global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        insertDefault();
        return;
    }

    auto variant_type = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type).getVariants()[src_global_discr];
    if (addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator[dynamic_src.variant_info.variant_names[src_global_discr]];
        variant_col.insertIntoVariantFrom(discr, src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(n));
        return;
    }

    /// We reached maximum number of variants and couldn't add new variant.
    /// This case should be really rare in real use cases.
    /// We should always be able to add String variant and cast inserted value to String.
    addStringVariant();
    auto tmp_variant_column = src_variant_col.getVariantByGlobalDiscriminator(src_global_discr).cloneEmpty();
    tmp_variant_column->insertFrom(src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(n));
    auto tmp_string_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    auto string_variant_discr = variant_info.variant_name_to_discriminator["String"];
    variant_col.insertIntoVariantFrom(string_variant_discr, *tmp_string_column, 0);
}

void ColumnDynamic::insertRangeFrom(const DB::IColumn & src_, size_t start, size_t length)
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_column->insertRangeFrom(*dynamic_src.variant_column, start, length);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);

    /// If variants are different, we need to extend our variant with new variants.
    if (auto global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertRangeFrom(*dynamic_src.variant_column, start, length, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// This case should be really rare in real use cases.
    /// Add String variant, cast all inserted variants in range to String and insert them to String variant.
    addStringVariant();
    auto tmp_variant_column_range = dynamic_src.variant_column->cut(start, length);
    const auto & tmp_variant_column_range_typed = assert_cast<const ColumnVariant &>(*tmp_variant_column_range);
    const auto & src_variant_type = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type);
    size_t num_variants = tmp_variant_column_range_typed.getNumVariants();
    /// Cast all variants to String
    Columns string_variants;
    string_variants.reserve(num_variants);
    for (size_t local_discr = 0; local_discr != num_variants; ++local_discr)
    {
        const auto & column = tmp_variant_column_range_typed.getVariantPtrByLocalDiscriminator(local_discr);
        const auto & type = src_variant_type.getVariant(tmp_variant_column_range_typed.globalDiscriminatorByLocal(local_discr));
        string_variants.push_back(castColumn(ColumnWithTypeAndName(column, type, ""), std::make_shared<DataTypeString>()));
    }

    /// Iterate through new discriminators and insert corresponding values to a String variant.
    const auto & local_discriminators = tmp_variant_column_range_typed.getLocalDiscriminators();
    const auto & offsets = tmp_variant_column_range_typed.getOffsets();
    auto string_variant_discr = variant_info.variant_name_to_discriminator["String"];
    for (size_t i = 0; i != local_discriminators.size(); ++i)
    {
        auto discr = local_discriminators[i];
        if (discr == ColumnVariant::NULL_DISCRIMINATOR)
            variant_col.insertDefault();
        else
            variant_col.insertIntoVariantFrom(string_variant_discr, *string_variants[discr], offsets[i]);
    }
}

void ColumnDynamic::insertManyFrom(const DB::IColumn & src_, size_t position, size_t length)
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_column->insertManyFrom(*dynamic_src.variant_column, position, length);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);

    /// If variants are different, we need to extend our variant with new variants.
    if (auto global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertManyFrom(*dynamic_src.variant_column, position, length, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// This case should be really rare in real use cases.
    /// We need to insert single value, try to add only corresponding variant.
    const auto & src_variant_col = assert_cast<const ColumnVariant &>(*dynamic_src.variant_column);
    auto src_global_discr = src_variant_col.globalDiscriminatorAt(position);
    if (src_global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        insertDefault();
        return;
    }

    auto variant_type = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type).getVariants()[src_global_discr];
    if (addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator[dynamic_src.variant_info.variant_names[src_global_discr]];
        variant_col.insertManyIntoVariantFrom(discr, src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(position), length);
        return;
    }

    addStringVariant();
    auto tmp_variant_column = src_variant_col.getVariantByGlobalDiscriminator(src_global_discr).cloneEmpty();
    tmp_variant_column->insertFrom(src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(position));
    auto tmp_string_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    auto string_variant_discr = variant_info.variant_name_to_discriminator["String"];
    variant_col.insertManyIntoVariantFrom(string_variant_discr, *tmp_string_column, 0, length);
}


StringRef ColumnDynamic::serializeValueIntoArena(size_t n, DB::Arena & arena, const char *& begin) const
{
    /// We cannot use Variant serialization here as it serializes discriminator + value,
    /// but Dynamic doesn't have fixed mapping discriminator <-> variant type
    /// as different Dynamic column can have different Variants.
    /// Instead, we serialize null bit + variant type name (size + bytes) + value.
    const auto & variant_col = assert_cast<const ColumnVariant &>(*variant_column);
    auto discr = variant_col.globalDiscriminatorAt(n);
    StringRef res;
    UInt8 null_bit = discr == ColumnVariant::NULL_DISCRIMINATOR;
    if (null_bit)
    {
        char * pos = arena.allocContinue(sizeof(UInt8), begin);
        memcpy(pos, &null_bit, sizeof(UInt8));
        res.data = pos;
        res.size = sizeof(UInt8);
        return res;
    }

    const auto & variant_name = variant_info.variant_names[discr];
    size_t variant_name_size = variant_name.size();
    char * pos = arena.allocContinue(sizeof(UInt8) + sizeof(size_t) + variant_name.size(), begin);
    memcpy(pos, &null_bit, sizeof(UInt8));
    memcpy(pos + sizeof(UInt8), &variant_name_size, sizeof(size_t));
    memcpy(pos + sizeof(UInt8) + sizeof(size_t), variant_name.data(), variant_name.size());
    res.data = pos;
    res.size = sizeof(UInt8) + sizeof(size_t) + variant_name.size();

    auto value_ref = variant_col.getVariantByGlobalDiscriminator(discr).serializeValueIntoArena(variant_col.offsetAt(n), arena, begin);
    res.data = value_ref.data - res.size;
    res.size += value_ref.size;
    return res;
}

const char * ColumnDynamic::deserializeAndInsertFromArena(const char * pos)
{
    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);
    UInt8 null_bit = unalignedLoad<UInt8>(pos);
    pos += sizeof(UInt8);
    if (null_bit)
    {
        insertDefault();
        return pos;
    }

    /// Read variant type name.
    const size_t variant_name_size = unalignedLoad<size_t>(pos);
    pos += sizeof(variant_name_size);
    String variant_name;
    variant_name.resize(variant_name_size);
    memcpy(variant_name.data(), pos, variant_name_size);
    pos += variant_name_size;
    /// If we already have such variant, just deserialize it into corresponding variant column.
    auto it = variant_info.variant_name_to_discriminator.find(variant_name);
    if (it != variant_info.variant_name_to_discriminator.end())
    {
        auto discr = it->second;
        return variant_col.deserializeVariantAndInsertFromArena(discr, pos);
    }

    /// If we don't have such variant, add it.
    auto variant_type = DataTypeFactory::instance().get(variant_name);
    if (likely(addNewVariant(variant_type)))
    {
        auto discr = variant_info.variant_name_to_discriminator[variant_name];
        return variant_col.deserializeVariantAndInsertFromArena(discr, pos);
    }

    /// We reached maximum number of variants and couldn't add new variant.
    /// This case should be really rare in real use cases.
    /// We should always be able to add String variant and cast inserted value to String.
    addStringVariant();
    /// Create temporary column of this variant type and deserialize value into it.
    auto tmp_variant_column = variant_type->createColumn();
    pos = tmp_variant_column->deserializeAndInsertFromArena(pos);
    /// Cast temporary column to String and insert this value into String variant.
    auto str_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    variant_col.insertIntoVariantFrom(variant_info.variant_name_to_discriminator["String"], *str_column, 0);
    return pos;
}

const char * ColumnDynamic::skipSerializedInArena(const char * pos) const
{
    UInt8 null_bit = unalignedLoad<UInt8>(pos);
    pos += sizeof(UInt8);
    if (null_bit)
        return pos;

    const size_t variant_name_size = unalignedLoad<size_t>(pos);
    pos += sizeof(variant_name_size);
    String variant_name;
    variant_name.resize(variant_name_size);
    memcpy(variant_name.data(), pos, variant_name_size);
    pos += variant_name_size;
    auto tmp_variant_column = DataTypeFactory::instance().get(variant_name)->createColumn();
    return tmp_variant_column->skipSerializedInArena(pos);
}

void ColumnDynamic::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto & variant_col = assert_cast<const ColumnVariant &>(*variant_column);
    auto discr = variant_col.globalDiscriminatorAt(n);
    if (discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        hash.update(discr);
        return;
    }

    hash.update(variant_info.variant_names[discr]);
    variant_col.getVariantByGlobalDiscriminator(discr).updateHashWithValue(variant_col.offsetAt(n), hash);
}

int ColumnDynamic::compareAt(size_t n, size_t m, const DB::IColumn & rhs, int nan_direction_hint) const
{
    const auto & left_variant = assert_cast<const ColumnVariant &>(*variant_column);
    const auto & right_dynamic = assert_cast<const ColumnDynamic &>(rhs);
    const auto & right_variant = assert_cast<const ColumnVariant &>(*right_dynamic.variant_column);

    auto left_discr = left_variant.globalDiscriminatorAt(n);
    auto right_discr = right_variant.globalDiscriminatorAt(m);

    /// Check if we have NULLs and return result based on nan_direction_hint.
    if (left_discr == ColumnVariant::NULL_DISCRIMINATOR && right_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return 0;
    else if (left_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return nan_direction_hint;
    else if (right_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return -nan_direction_hint;

    /// If rows have different types, we compare type names.
    if (variant_info.variant_names[left_discr] != right_dynamic.variant_info.variant_names[right_discr])
        return variant_info.variant_names[left_discr] < right_dynamic.variant_info.variant_names[right_discr] ? -1 : 1;

    /// If rows have the same types, compare actual values from corresponding variants.
    return left_variant.getVariantByGlobalDiscriminator(left_discr).compareAt(left_variant.offsetAt(n), right_variant.offsetAt(m), right_variant.getVariantByGlobalDiscriminator(right_discr), nan_direction_hint);
}

ColumnPtr ColumnDynamic::compress() const
{
    ColumnPtr variant_compressed = variant_column->compress();
    size_t byte_size = variant_compressed->byteSize();
    return ColumnCompressed::create(size(), byte_size,
        [my_variant_compressed = std::move(variant_compressed), my_variant_info = variant_info]() mutable
        {
            return ColumnDynamic::create(my_variant_compressed->decompress(), my_variant_info);
        });
}

void ColumnDynamic::takeDynamicStructureFromSourceColumns(const DB::Columns & source_columns)
{
    /// During serialization of Dynamic column in MergeTree all Dynamic columns
    /// in single part must have the same structure (the same variants). During merge
    /// resulting column is constructed by inserting from source columns,
    /// but it may happen that resulting column doesn't have rows from all source parts
    /// but only from subset of them, and as a result some variants could be missing
    /// and structures of resulting column may differ.
    /// To solve this problem, before merge we create empty resulting column and use this method
    /// to take dynamic structure from all source column even if we won't insert
    /// rows from some of them.

    /// Iterate through all source Dynamic columns and construct resulting Variant type.
    for (const auto & source_column : source_columns)
    {
        const auto & source_variant_info = assert_cast<const ColumnDynamic &>(*source_column).getVariantInfo();
        if (!combineVariants(source_variant_info))
        {
            /// We couldn't combine 2 Variants because the total number of variants
            /// exceeds the limit. This case should be really rare in real use cases.
            /// We still want to have as much variants from source columns as possible in the resulting column,
            /// so, we iterate through all variants and try to add them one by one until we reach the limit.
            const auto & source_variants = assert_cast<const DataTypeVariant &>(*source_variant_info.variant_type).getVariants();
            for (const auto & source_variant : source_variants)
            {
                if (!addNewVariant(source_variant))
                    break;
            }

            /// We reached the limit, but it should be always possible to add String variant in which
            /// we will insert data from variants we couldn't add.
            addStringVariant();
            break;
        }
    }

    /// Now we have the resulting Variant that will be used in all merged columns.
    /// Variants can also contain Dynamic columns inside, we should collect
    /// all source variants that will be used in the resulting merged column
    /// and call takeDynamicStructureFromSourceColumns on all resulting variants.
    std::vector<Columns> variants_source_columns;
    variants_source_columns.resize(variant_info.variant_names.size());
    for (const auto & source_column : source_columns)
    {
        const auto & source_dynamic_column = assert_cast<const ColumnDynamic &>(*source_column);
        const auto & source_variant_info = source_dynamic_column.getVariantInfo();
        for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
        {
            /// Try to find this variant in current source column.
            auto it = source_variant_info.variant_name_to_discriminator.find(variant_info.variant_names[i]);
            if (it != source_variant_info.variant_name_to_discriminator.end())
                variants_source_columns[i].push_back(source_dynamic_column.getVariantColumn().getVariantPtrByGlobalDiscriminator(it->second));
        }
    }

    auto & variant_col = getVariantColumn();
    for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
        variant_col.getVariantByGlobalDiscriminator(i).takeDynamicStructureFromSourceColumns(variants_source_columns[i]);
}

void ColumnDynamic::applyNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    assert_cast<ColumnVariant &>(*variant_column).applyNullMap(null_map);
}

void ColumnDynamic::applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    assert_cast<ColumnVariant &>(*variant_column).applyNegatedNullMap(null_map);
}

}
