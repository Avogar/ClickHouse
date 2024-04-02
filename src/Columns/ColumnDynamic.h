#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ColumnDynamic final : public COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>
{
private:
    friend class COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>;

    struct VariantInfo
    {
        DataTypePtr variant_type;
        /// Name of the whole variant to not call getName() every time.
        String variant_name;
        /// Store names of variants to not call getName() every time on variants.
        Names variant_names;
        /// Store mapping (variant name) -> (global discriminator).
        /// It's used during variant extension.
        std::unordered_map<String, UInt8> variant_name_to_discriminator;
    };

    ColumnDynamic();
    ColumnDynamic(MutableColumnPtr variant_column_, const VariantInfo & variant_info_);

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>;
    static Ptr create(const ColumnPtr & variant_column_, const VariantInfo & variant_info_)
    {
        return ColumnDynamic::create(variant_column_->assumeMutable(), variant_info_);
    }

    static MutablePtr create(MutableColumnPtr variant_column_, const VariantInfo & variant_info_)
    {
        return Base::create(std::move(variant_column_), variant_info_);
    }

    static MutablePtr create(MutableColumnPtr variant_column_, const DataTypePtr & variant_type);

    static ColumnPtr create(ColumnPtr variant_column_, const DataTypePtr & variant_type)
    {
        return create(variant_column_->assumeMutable(), variant_type);
    }

    static MutablePtr create()
    {
        return Base::create();
    }

    const char * getFamilyName() const override
    {
        return "Dynamic";
    }

    TypeIndex getDataType() const override
    {
        return TypeIndex::Dynamic;
    }

    MutableColumnPtr cloneEmpty() const override
    {
        /// Keep current dynamic types.
        return Base::create(variant_column->cloneEmpty(), variant_info);
    }

    MutableColumnPtr cloneResized(size_t size) const override
    {
        return Base::create(variant_column->cloneResized(size), variant_info);
    }

    size_t size() const override
    {
        return variant_column->size();
    }

    Field operator[](size_t n) const override
    {
        return (*variant_column)[n];
    }

    void get(size_t n, Field & res) const override
    {
        variant_column->get(n, res);
    }

    bool isDefaultAt(size_t n) const override
    {
        return variant_column->isDefaultAt(n);
    }

    bool isNullAt(size_t n) const override
    {
        return variant_column->isNullAt(n);
    }

    StringRef getDataAt(size_t n) const override
    {
        return variant_column->getDataAt(n);
    }

    void insertData(const char * pos, size_t length) override
    {
        return variant_column->insertData(pos, length);
    }

    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;

    void insertDefault() override
    {
        variant_column->insertDefault();
    }

    void insertManyDefaults(size_t length) override
    {
        variant_column->insertManyDefaults(length);
    }

    void popBack(size_t n) override
    {
        variant_column->popBack(n);
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    void updateWeakHash32(WeakHash32 & hash) const override
    {
        variant_column->updateWeakHash32(hash);
    }

    void updateHashFast(SipHash & hash) const override
    {
        variant_column->updateHashFast(hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return create(variant_column->filter(filt, result_size_hint), variant_info);
    }

    void expand(const Filter & mask, bool inverted) override
    {
        variant_column->expand(mask, inverted);
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return create(variant_column->permute(perm, limit), variant_info);
    }

    ColumnPtr index(const IColumn & indexes, size_t limit) const override
    {
        return create(variant_column->index(indexes, limit), variant_info);
    }

    ColumnPtr replicate(const Offsets & replicate_offsets) const override
    {
        return create(variant_column->replicate(replicate_offsets), variant_info);
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        MutableColumns scattered_variant_columns = variant_column->scatter(num_columns, selector);
        MutableColumns scattered_columns;
        scattered_columns.reserve(num_columns);
        for (auto & scattered_variant_column : scattered_variant_columns)
            scattered_columns.emplace_back(create(std::move(scattered_variant_column), variant_info));

        return scattered_columns;
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    bool hasEqualValues() const override
    {
        return variant_column->hasEqualValues();
    }

    void getExtremes(Field & min, Field & max) const override
    {
        variant_column->getExtremes(min, max);
    }

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override
    {
        variant_column->getPermutation(direction, stability, limit, nan_direction_hint, res);
    }

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                           size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override
    {
        variant_column->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
    }

    void reserve(size_t n) override
    {
        variant_column->reserve(n);
    }

    void ensureOwnership() override
    {
        variant_column->ensureOwnership();
    }

    size_t byteSize() const override
    {
        return variant_column->byteSize();
    }

    size_t byteSizeAt(size_t n) const override
    {
        return variant_column->byteSizeAt(n);
    }

    size_t allocatedBytes() const override
    {
        return variant_column->allocatedBytes();
    }

    void protect() override
    {
        variant_column->protect();
    }

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        callback(variant_column);
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*variant_column);
        variant_column->forEachSubcolumnRecursively(callback);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnDynamic);
    }

    ColumnPtr compress() const override;

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return variant_column->getRatioOfDefaultRows(sample_ratio);
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return variant_column->getNumberOfDefaultRows();
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        variant_column->getIndicesOfNonDefaultRows(indices, from, limit);
    }

    void finalize() override
    {
        variant_column->finalize();
    }

    bool isFinalized() const override
    {
        return variant_column->isFinalized();
    }

    /// Apply null map to a nested Variant column.
    void applyNullMap(const ColumnVector<UInt8>::Container & null_map);
    void applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map);

    const VariantInfo & getVariantInfo() const { return variant_info; }

    const ColumnPtr & getVariantColumnPtr() const { return variant_column; }
    ColumnPtr & getVariantColumnPtr() { return variant_column; }

    const ColumnVariant & getVariantColumn() const { return assert_cast<const ColumnVariant &>(*variant_column); }
    ColumnVariant & getVariantColumn() { return assert_cast<ColumnVariant &>(*variant_column); }

    bool addNewVariant(const DataTypePtr & new_variant);
    void addStringVariant();

    bool hasDynamicStructure() const override { return true; }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override;

private:
    std::vector<UInt8> * combineVariants(const VariantInfo & other_variant_info);

    WrappedPtr variant_column;
    /// Store the type of current variant with some additional information.
    VariantInfo variant_info;

    std::unordered_map<String, std::vector<UInt8>> variant_mappings_cache;
};

}
