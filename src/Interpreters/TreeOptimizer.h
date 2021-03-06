#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Part of of Tree Rewriter (SyntaxAnalyzer) that optimizes AST.
/// Query should be ready to execute either before either after it. But resulting query could be faster.
class TreeOptimizer
{
public:
    static void apply(
        ASTPtr & query,
        Aliases & aliases,
        const NameSet & source_columns_set,
        const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns,
        ContextConstPtr context,
        const StorageMetadataPtr & metadata_snapshot,
        bool & rewrite_subqueries);

    static void optimizeIf(ASTPtr & query, Aliases & aliases, bool if_chain_to_multiif);
};

}
