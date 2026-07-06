#pragma once

#include "loki/auth.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include <string>
#include <vector>

namespace duckdb {

// One tenant of an attached Loki (DESIGN.md §3.6). Single-tenant Loki → one tenant named
// "public"; multi-tenant → one per `tenants` attach option, the schema named after the tenant id.
struct LokiTenant {
	std::string schema_name;                // catalog schema name (e.g. "public" or the tenant id)
	std::string tenant_id;                  // X-Scope-OrgID for this schema ("" = none)
	std::vector<std::string> label_columns; // labels promoted to pushable VARCHAR columns on `logs`
};

// Connection + layout resolved once at attach time.
struct LokiCatalogConfig {
	std::string endpoint;            // trimmed Loki base URL
	loki::AuthConfig auth;           // base auth (token/basic/headers); tenant filled in per-schema
	std::vector<LokiTenant> tenants; // ≥1; single {"public", <tenant option>, <labels>} by default
};

class LokiSchemaEntry;
class LokiLogsTable;
class LokiLabelsTable;
class LokiSeriesTable;

// A read-only catalog exposing a Loki instance as an attached database. The `logs` table's scan is
// backed by the same pushdown core as loki() — the catalog is a thin adapter (DESIGN.md §3.6).
class LokiCatalog : public Catalog {
public:
	LokiCatalog(AttachedDatabase &db, LokiCatalogConfig config);
	~LokiCatalog() override;

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "loki";
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void DropSchema(ClientContext &context, DropInfo &info) override;

	// Read-only: no physical mutation plans.
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		return DatabaseSize();
	}
	bool InMemory() override {
		return false;
	}
	string GetDBPath() override {
		return config.endpoint;
	}
	string GetDefaultSchema() const override;

private:
	LokiCatalogConfig config;
	vector<unique_ptr<LokiSchemaEntry>> schemas;
};

// One schema per tenant, holding the fixed `logs` table (DESIGN.md §3.6).
class LokiSchemaEntry : public SchemaCatalogEntry {
public:
	LokiSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, const LokiCatalogConfig &catalog_config,
	                const LokiTenant &tenant);

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	// Read-only: every mutation throws.
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	unique_ptr<LokiLogsTable> logs_table;
	unique_ptr<LokiLabelsTable> labels_table;
	unique_ptr<LokiSeriesTable> series_table;
};

// The `logs` virtual table. Its scan reuses the pushdown core (DESIGN.md §3.6): GetScanFunction
// hands DuckDB a pre-populated LokiScanBindData plus a bind-less loki() TableFunction.
class LokiLogsTable : public TableCatalogEntry {
public:
	LokiLogsTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, std::string endpoint,
	              loki::AuthConfig auth, std::vector<std::string> label_columns);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override {
		return TableStorageInfo();
	}
	// No rowid/virtual columns — parity with the loki() table function scan.
	virtual_column_map_t GetVirtualColumns() const override {
		return virtual_column_map_t();
	}
	vector<column_t> GetRowIdColumns() const override {
		return vector<column_t>();
	}

private:
	std::string endpoint;
	loki::AuthConfig auth;
	std::vector<std::string> label_columns;
};

// The optional read-only `labels` table (DESIGN.md §3.6): one row per label name, backed by
// GET /labels — for exploration/autocomplete. No pushdown (a plain full enumeration).
class LokiLabelsTable : public TableCatalogEntry {
public:
	LokiLabelsTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, std::string endpoint,
	                loki::AuthConfig auth);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override {
		return TableStorageInfo();
	}
	virtual_column_map_t GetVirtualColumns() const override {
		return virtual_column_map_t();
	}
	vector<column_t> GetRowIdColumns() const override {
		return vector<column_t>();
	}

private:
	std::string endpoint;
	loki::AuthConfig auth;
};

// The optional read-only `series` table (DESIGN.md §3.6): one row per matching series, backed by
// GET /series. Loki refuses /series without a selector, so — like `logs` — it promotes label
// columns and reuses the pushdown translation to synthesize the `match[]` selector from WHERE; a
// scan without a pushable label predicate hard-errors.
class LokiSeriesTable : public TableCatalogEntry {
public:
	LokiSeriesTable(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, std::string endpoint,
	                loki::AuthConfig auth, std::vector<std::string> label_columns);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override {
		return TableStorageInfo();
	}
	virtual_column_map_t GetVirtualColumns() const override {
		return virtual_column_map_t();
	}
	vector<column_t> GetRowIdColumns() const override {
		return vector<column_t>();
	}

private:
	std::string endpoint;
	loki::AuthConfig auth;
	std::vector<std::string> label_columns;
};

// A trivial read-only transaction manager: the Loki catalog never mutates, so transactions carry
// no state and commit/rollback are no-ops. (DuckTransactionManager can't be reused — it asserts a
// DuckCatalog.)
class LokiTransactionManager : public TransactionManager {
public:
	explicit LokiTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
	}
	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force) override;

private:
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

// Register the `loki` storage extension so `ATTACH ... (TYPE loki)` works (DESIGN.md §3.6).
void RegisterLokiStorageExtension(DBConfig &config);

} // namespace duckdb
