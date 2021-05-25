#include <Columns/MaskOperations.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename T>
void expandDataByMask(PaddedPODArray<T> & data, const PaddedPODArray<UInt8> & mask, bool inverted)
{
    if (mask.size() < data.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    int from = data.size() - 1;
    int index = mask.size() - 1;
    data.resize(mask.size());
    while (index >= 0)
    {
        if (mask[index] ^ inverted)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            data[index] = data[from];
            --from;
        }

        --index;
    }

    if (from != -1)
        throw Exception("Not enough bytes in mask", ErrorCodes::LOGICAL_ERROR);
}

/// Explicit instantiations - not to place the implementation of the function above in the header file.
#define INSTANTIATE(TYPE) \
template void expandDataByMask<TYPE>(PaddedPODArray<TYPE> &, const PaddedPODArray<UInt8> &, bool);

INSTANTIATE(UInt8)
INSTANTIATE(UInt16)
INSTANTIATE(UInt32)
INSTANTIATE(UInt64)
INSTANTIATE(UInt128)
INSTANTIATE(UInt256)
INSTANTIATE(Int8)
INSTANTIATE(Int16)
INSTANTIATE(Int32)
INSTANTIATE(Int64)
INSTANTIATE(Int128)
INSTANTIATE(Int256)
INSTANTIATE(Float32)
INSTANTIATE(Float64)
INSTANTIATE(Decimal32)
INSTANTIATE(Decimal64)
INSTANTIATE(Decimal128)
INSTANTIATE(Decimal256)
INSTANTIATE(DateTime64)
INSTANTIATE(char *)
INSTANTIATE(UUID)

#undef INSTANTIATE

void expandOffsetsByMask(PaddedPODArray<UInt64> & offsets, const PaddedPODArray<UInt8> & mask, bool inverted)
{
    if (mask.size() < offsets.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    int index = mask.size() - 1;
    int from = offsets.size() - 1;
    offsets.resize(mask.size());
    UInt64 prev_offset = offsets[from];
    while (index >= 0)
    {
        if (mask[index] ^ inverted)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            offsets[index] = offsets[from];
            --from;
            prev_offset = offsets[from];
        }
        else
            offsets[index] = prev_offset;
        --index;
    }

    if (from != -1)
        throw Exception("Not enough bytes in mask", ErrorCodes::LOGICAL_ERROR);
}

void expandColumnByMask(const ColumnPtr & column, const PaddedPODArray<UInt8>& mask, bool inverted)
{
    column->assumeMutable()->expand(mask, inverted);
}

MaskInfo getMaskFromColumn(
    const ColumnPtr & column,
    PaddedPODArray<UInt8> & res,
    bool inverted,
    const PaddedPODArray<UInt8> * mask_used_in_expanding,
    UInt8 default_value_in_expanding,
    bool inverted_mask_used_in_expanding,
    UInt8 null_value,
    const PaddedPODArray<UInt8> * null_bytemap,
    bool is_expanded,
    PaddedPODArray<UInt8> * nulls)
{
    size_t size = is_expanded ? column->size() : mask_used_in_expanding->size();
    res.resize(size);

    if (column->onlyNull())
    {
//        res.resize_fill(size, inverted ? !null_value : null_value);
        bool value = inverted ? !null_value : null_value;
        std::fill(res.begin(), res.end(), inverted ? !null_value : null_value);
        if (nulls)
            std::fill(nulls->begin(), nulls->end(), 1);

//        for (size_t i = 0; i != size; ++i)
//        {
//            if (mask_used_in_expanding && (!(*mask_used_in_expanding)[i] ^ inverted_mask_used_in_expanding))
//                res[i] = inverted ? !default_value_in_expanding : default_value_in_expanding;
//            else
//                res[i] = inverted ? !null_value : null_value;
//            if (nulls)
//                (*nulls)[i] = 1;
//        }

        return {value, !value};
    }

    if (const auto * col = checkAndGetColumn<ColumnNullable>(*column))
    {
        const PaddedPODArray<UInt8> & null_map = checkAndGetColumn<ColumnUInt8>(*col->getNullMapColumnPtr())->getData();
        return getMaskFromColumn(col->getNestedColumnPtr(), res, inverted, mask_used_in_expanding, default_value_in_expanding, inverted_mask_used_in_expanding, null_value, &null_map, is_expanded);
    }

    /// Some columns doesn't implement getBool() method and we cannot
    /// convert them to mask, throw an exception in this case.
    try
    {
        if (const auto * col = checkAndGetColumn<ColumnConst>(*column))
        {
            bool value = col->getBool(0);
            std::fill(res.begin(), res.end(), inverted ? !value : value);
            if (!value && nulls)
                std::fill(nulls->begin(), nulls->end(), 0);
            return;
        }


        if (res.size() != size)
            res.resize(size);

        size_t column_index = 0;
        for (size_t i = 0; i != size; ++i)
        {
            if (mask_used_in_expanding && (!(*mask_used_in_expanding)[i] ^ inverted_mask_used_in_expanding))
                res[i] = inverted ? !default_value_in_expanding : default_value_in_expanding;
            else if (null_bytemap && (*null_bytemap)[i])
            {
                res[i] = inverted ? !null_value : null_value;
                if (nulls)
                    (*nulls)[i] = 1;
            }
            else
                res[i] = inverted ? !column->getBool(column_index) : column->getBool(column_index);
            if (!mask_used_in_expanding || is_expanded || ((*mask_used_in_expanding)[i] ^ inverted_mask_used_in_expanding))
                ++column_index;
            if (!res[i] && nulls)
                (*nulls)[i] = 0;
        }

    }
    catch (...)
    {
        throw Exception("Cannot convert column " + column.get()->getName() + " to mask", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
}

void disjunctionMasks(PaddedPODArray<UInt8> & mask1, const PaddedPODArray<UInt8> & mask2)
{
    if (mask1.size() != mask2.size())
        throw Exception("Cannot make a disjunction of masks, they have different sizes", ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i != mask1.size(); ++i)
        mask1[i] = mask1[i] | mask2[i];
}

bool maskedExecute(ColumnWithTypeAndName & column, const PaddedPODArray<UInt8> & mask, bool inverted, bool expand)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function || !column_function->isShortCircuitArgument())
        return true;

    auto filtered = column_function->filter(mask, -1, inverted);
    auto result = typeid_cast<const ColumnFunction *>(filtered.get())->reduce();
    bool is_full = false;
    if (expand)
    {
        expandColumnByMask(result.column, mask, inverted);
        is_full = true;
    }
    column = std::move(result);
    return is_full;
}

void executeColumnIfNeeded(ColumnWithTypeAndName & column)
{
    const auto * column_function = checkAndGetColumn<ColumnFunction>(*column.column);
    if (!column_function || !column_function->isShortCircuitArgument())
        return;

    column = typeid_cast<const ColumnFunction *>(column_function)->reduce();
}


int checkShirtCircuitArguments(const ColumnsWithTypeAndName & arguments)
{
    int last_short_circuit_argument_index = -1;
    for (size_t i = 0; i != arguments.size(); ++i)
    {
        const auto * column_func = checkAndGetColumn<ColumnFunction>(*arguments[i].column);
        if (column_func && column_func->isShortCircuitArgument())
            last_short_circuit_argument_index = i;
    }

    return last_short_circuit_argument_index;
}

}
