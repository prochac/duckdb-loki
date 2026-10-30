#pragma once

#include "loki/auth.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/table_storage_info.hpp"
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
	// ATTACH passed an explicit `labels` list: the promoted columns are pinned, so there is nothing
	// to discover and nothing to refresh.
	bool labels_pinned = false;
	// How long a discovered label set may be reused before the next statement re-fetches it
	// (DESIGN.md §11.8). 0 — the default — disables time-based refresh entirely, leaving
	// PRAGMA loki_refresh_labels as the only way to pick up new labels.
	int64_t label_cache_ttl_ns = 0;
	// Cap on the `series` table's single-shot enumeration (DESIGN.md §3.6).
	idx_t series_max_rows = 100000;
};

class LokiSchemaEntry;
class LokiLogsTable;
class LokiLabelsTable;
class LokiSeriesTable;

// One generation of the catalog's schema tree, immutable once published.
//
// A label refresh (DESIGN.md §11.8) changes the promoted label columns, and therefore the `logs`
// and `series` column lists — which are fixed at TableCatalogEntry construction and are read
// concurrently by binders and by information_schema/duckdb_columns() scans at execution time.
// Mutating them in place would be a data race, so a refresh builds a whole new snapshot and swaps
// it in; readers keep using the generation they started with.
struct LokiCatalogSnapshot {
	idx_t version = 0;
	std::vector<std::vector<std::string>> label_columns; // parallel to LokiCatalogConfig::tenants
	vector<unique_ptr<LokiSchemaEntry>> schemas;
};

// A transaction over the loki catalog. It pins the snapshot current when it started, so every
// lookup within one statement — or one BEGIN…COMMIT — sees a single, stable set of catalog
// entries even if a refresh publishes a new generation meanwhile.
class LokiTransaction : public Transaction {
public:
	LokiTransaction(TransactionManager &manager, ClientContext &context,
	                shared_ptr<const LokiCatalogSnapshot> snapshot_p)
	    : Transaction(manager, context), snapshot(std::move(snapshot_p)) {
	}

	shared_ptr<const LokiCatalogSnapshot> snapshot;
};

// A read-only catalog exposing a Loki instance as an attached database. The `logs` table's scan is
// backed by the same pushdown core as loki() — the catalog is a thin adapter (DESIGN.md §3.6).
class LokiCatalog : public Catalog {
public:
	LokiCatalog(AttachedDatabase &db, LokiCatalogConfig config);
	~LokiCatalog() override;

	// Take a reference on the currently published snapshot, for a transaction to hold onto.
	shared_ptr<const LokiCatalogSnapshot> PinCurrentSnapshot();

	// TTL path (DESIGN.md §11.8): re-fetch the label set if `label_cache_ttl` has elapsed. Called
	// once per transaction start. A failure here is swallowed and backed off — an unreachable Loki
	// must not make an already-attached database unqueryable — so the old snapshot stays live.
	//
	// It must never call Transaction::Get or Catalog::GetCatalogTransaction: it runs while
	// MetaTransaction::lock is held, and re-entering GetTransaction would self-deadlock.
	void MaybeRefreshLabels(ClientContext &context);
	// PRAGMA path: re-fetch now, blocking, and let failures surface to the user who asked.
	void RefreshLabels(ClientContext &context);
	bool LabelsPinned() const {
		return config.labels_pinned;
	}

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
	optional<Identifier> GetDefaultSchema() const override;

private:
	// Build a generation from a per-tenant label-column list (parallel to config.tenants).
	shared_ptr<const LokiCatalogSnapshot> BuildSnapshot(std::vector<std::vector<std::string>> labels, idx_t version);
	// The snapshot a read should use: the one pinned by this transaction, else the published one.
	// Returned by value so the caller's use cannot race a concurrent swap.
	shared_ptr<const LokiCatalogSnapshot> PinnedSnapshot(optional_ptr<Transaction> transaction);
	// Fetch every tenant's labels and, if they differ from the current generation, publish a new
	// one. All-or-nothing: a tenant that fails aborts the whole refresh rather than publishing a
	// half-updated catalog. Returns true when a new generation was published. Caller holds
	// `refresh_lock`; throws on any transport/parse failure.
	bool DoRefresh(bool short_timeouts);

	// Immutable after attach: endpoint, auth, and the tenant list. Only the tenants' *label columns*
	// can change (on refresh), and those live in the snapshot — so this needs no lock.
	LokiCatalogConfig config;

	mutex snapshot_lock;
	shared_ptr<const LokiCatalogSnapshot> current;
	// Superseded generations, kept alive until no transaction holds them (pins are only ever taken
	// from `current`, so use_count()==1 here proves nobody is reading it).
	vector<shared_ptr<const LokiCatalogSnapshot>> retired;

	// Serialises the refresh HTTP round-trip. Never held together with `snapshot_lock`.
	mutex refresh_lock;
	// Wall-clock ns after which the TTL path should re-fetch; also carries the failure backoff.
	atomic<int64_t> next_check_ns;
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
	const ColumnList &GetColumns() const override {
		return columns;
	}
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
	// TableCatalogEntry no longer stores the column list; each virtual table owns its own.
	ColumnList columns;
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
	const ColumnList &GetColumns() const override {
		return columns;
	}
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
	// TableCatalogEntry no longer stores the column list; each virtual table owns its own.
	ColumnList columns;
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
	                loki::AuthConfig auth, std::vector<std::string> label_columns, idx_t max_rows);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override {
		return nullptr;
	}
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	const ColumnList &GetColumns() const override {
		return columns;
	}
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
	// TableCatalogEntry no longer stores the column list; each virtual table owns its own.
	ColumnList columns;
	std::string endpoint;
	loki::AuthConfig auth;
	std::vector<std::string> label_columns;
	idx_t max_rows;
};

// A trivial read-only transaction manager: the Loki catalog never mutates, so transactions carry
// no state and commit/rollback are no-ops. (DuckTransactionManager can't be reused — it asserts a
// DuckCatalog.)
class LokiTransactionManager : public TransactionManager {
public:
	LokiTransactionManager(AttachedDatabase &db, LokiCatalog &catalog_p) : TransactionManager(db), catalog(catalog_p) {
	}
	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force) override;

private:
	// Safe to hold by reference: AttachedDatabase declares `catalog` before `transaction_manager`,
	// so the manager is destroyed first.
	LokiCatalog &catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<Transaction>> transactions;
};

// Register the `loki` storage extension so `ATTACH ... (TYPE loki)` works (DESIGN.md §3.6), plus
// `PRAGMA loki_refresh_labels [('db')]` for re-discovering label columns (DESIGN.md §11.8).
void RegisterLokiStorageExtension(ExtensionLoader &loader);

} // namespace duckdb
