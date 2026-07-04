#include "loki/loki_scan.hpp"

#include "loki/auth.hpp"
#include "loki/http_client.hpp"
#include "loki/http_request.hpp"
#include "loki/logql.hpp"
#include "loki/pager.hpp"
#include "loki/parse.hpp"
#include "loki/time_bounds.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace duckdb {

namespace {

constexpr int64_t NANOS_PER_HOUR = 3600LL * 1000LL * 1000LL * 1000LL;
constexpr int64_t DEFAULT_LIMIT = 5000;

struct LokiScanBindData : public TableFunctionData {
	std::string endpoint;
	std::string query; // raw LogQL (loki_scan); for loki() it's built at init from the selector
	int64_t start_ns = 0;
	int64_t end_ns = 0;
	int64_t limit = DEFAULT_LIMIT;
	std::string direction = "backward";
	loki::AuthConfig auth;
	std::vector<std::string> label_columns; // labels promoted to top-level columns, in declared order

	// --- Filter pushdown (loki(), DESIGN.md §4). Untouched by loki_scan (raw LogQL). ---
	bool pushdown_mode = false; // true for loki(): assemble the query from selector + WHERE
	std::string base_selector;  // mandatory base stream selector, refined by pushdown
	std::vector<loki::LabelMatcher> pushed_matchers;
	std::vector<loki::LineFilter> pushed_line_filters;
	bool has_param_start = false; // start/end came from an explicit start:=/end:= argument
	bool has_param_end = false;
	// Time bounds gathered from WHERE (intersected across predicates); sentinels mean "none".
	int64_t pushed_start_ns = std::numeric_limits<int64_t>::min();
	int64_t pushed_end_ns = std::numeric_limits<int64_t>::max();
};

struct LokiScanGlobalState : public GlobalTableFunctionState {
	LokiScanGlobalState(std::string endpoint_p, std::vector<std::pair<std::string, std::string>> headers_p,
	                    loki::StreamPager pager_p)
	    : endpoint(std::move(endpoint_p)), headers(std::move(headers_p)), pager(std::move(pager_p)) {
	}

	std::string endpoint;
	std::vector<std::pair<std::string, std::string>> headers;
	loki::StreamPager pager;
	std::vector<loki::LokiRow> buffer;
	idx_t buf_offset = 0;
	std::vector<column_t> column_ids; // projected bind-schema columns to emit, in output order

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Convert a bound TIMESTAMP/TIMESTAMPTZ/VARCHAR parameter to a nanosecond Unix epoch.
// Casting to TIMESTAMP yields UTC microseconds, which GetEpochNanoSeconds scales to ns.
int64_t ValueToEpochNs(const Value &value) {
	Value as_ts = value.DefaultCastAs(LogicalType::TIMESTAMP);
	auto ts = as_ts.GetValue<timestamp_t>();
	return Timestamp::GetEpochNanoSeconds(ts);
}

// Convert a bound timestamp value to an exact nanosecond epoch (no microsecond rounding),
// used for pushed-down WHERE bounds where the column itself is TIMESTAMP_NS.
int64_t ValueToEpochNsExact(const Value &value) {
	Value as_ns = value.DefaultCastAs(LogicalType::TIMESTAMP_NS);
	return as_ns.GetValueUnsafe<int64_t>();
}

// Resolve a start/end bound. An INTERVAL is an offset added to now() (so `-INTERVAL 2 HOUR`
// means two hours ago); any timestamp-like value is taken as an absolute instant.
int64_t ResolveTimeBound(const Value &value, int64_t now_ns) {
	if (value.type().id() == LogicalTypeId::INTERVAL) {
		auto iv = value.GetValue<interval_t>();
		try {
			return now_ns + loki::IntervalToNanos(iv.months, iv.days, iv.micros);
		} catch (const std::exception &e) {
			throw BinderException(e.what());
		}
	}
	return ValueToEpochNs(value);
}

// Extract key/value pairs from a MAP(VARCHAR, VARCHAR) Value into a header list.
void AppendHeadersFromMap(const Value &map_value, std::vector<std::pair<std::string, std::string>> &out) {
	if (map_value.IsNull()) {
		return;
	}
	for (const auto &entry : ListValue::GetChildren(map_value)) {
		auto kv = StructValue::GetChildren(entry);
		out.emplace_back(kv[0].ToString(), kv[1].ToString());
	}
}

// Look up a named parameter, returning nullptr when absent or explicitly NULL.
const Value *FindParam(TableFunctionBindInput &input, const char *name) {
	auto it = input.named_parameters.find(name);
	if (it == input.named_parameters.end() || it->second.IsNull()) {
		return nullptr;
	}
	return &it->second;
}

// Copy a `loki` secret's fields into the connection config being assembled.
void ReadLokiSecret(const KeyValueSecret &kv, std::string &endpoint, loki::AuthConfig &auth,
                    std::vector<std::pair<std::string, std::string>> &headers) {
	Value v;
	if (kv.TryGetValue("endpoint", v) && !v.IsNull()) {
		endpoint = v.ToString();
	}
	if (kv.TryGetValue("token", v) && !v.IsNull()) {
		auth.token = v.ToString();
	}
	if (kv.TryGetValue("username", v) && !v.IsNull()) {
		auth.username = v.ToString();
	}
	if (kv.TryGetValue("password", v) && !v.IsNull()) {
		auth.password = v.ToString();
	}
	if (kv.TryGetValue("tenant", v) && !v.IsNull()) {
		auth.tenant = v.ToString();
	}
	if (kv.TryGetValue("headers", v)) {
		AppendHeadersFromMap(v, headers);
	}
}

// Shared bind work for loki_scan (raw) and loki (pushdown): resolve the connection/auth from a
// secret plus inline overrides, the row limit and paging direction, the time bounds, the promoted
// label columns, and finally the output schema. The caller sets the query source (raw `query`
// or `base_selector`) before calling this. The function name for error messages is `fn`.
void LokiResolveCommon(ClientContext &context, TableFunctionBindInput &input, LokiScanBindData &result,
                       vector<LogicalType> &return_types, vector<string> &names, const char *fn) {
	// --- Resolve connection config: a `loki` secret, refined by inline overrides. ---
	std::string endpoint;
	std::vector<std::pair<std::string, std::string>> secret_headers;

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (auto *secret_param = FindParam(input, "secret")) {
		// Explicit secret name — must exist.
		auto name = secret_param->ToString();
		auto entry = secret_manager.GetSecretByName(transaction, name);
		if (!entry) {
			throw BinderException("%s: no secret named '%s' found", fn, name);
		}
		ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(*entry->secret), endpoint, result.auth, secret_headers);
	} else if (auto entry = secret_manager.GetSecretByName(transaction, "loki")) {
		// No explicit secret: fall back to a default secret named "loki" if one exists.
		ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(*entry->secret), endpoint, result.auth, secret_headers);
	}

	// Inline overrides take precedence over the secret.
	if (auto *p = FindParam(input, "endpoint")) {
		endpoint = p->ToString();
	}
	if (auto *p = FindParam(input, "token")) {
		result.auth.token = p->ToString();
	}
	if (auto *p = FindParam(input, "username")) {
		result.auth.username = p->ToString();
	}
	if (auto *p = FindParam(input, "password")) {
		result.auth.password = p->ToString();
	}
	if (auto *p = FindParam(input, "tenant")) {
		result.auth.tenant = p->ToString();
	}
	if (auto *p = FindParam(input, "headers")) {
		// Inline headers replace the secret's headers.
		secret_headers.clear();
		AppendHeadersFromMap(*p, secret_headers);
	}
	result.auth.extra_headers = std::move(secret_headers);

	if (endpoint.empty()) {
		throw BinderException("%s requires an endpoint: pass endpoint := 'http://localhost:3100' or "
		                      "reference a secret (secret := 'my_loki', or a default secret named 'loki') "
		                      "created with CREATE SECRET (TYPE loki, ENDPOINT ...)",
		                      fn);
	}
	while (!endpoint.empty() && endpoint.back() == '/') {
		endpoint.pop_back();
	}
	result.endpoint = std::move(endpoint);

	if (auto *p = FindParam(input, "limit")) {
		result.limit = p->GetValue<int64_t>();
		if (result.limit <= 0) {
			throw BinderException("%s 'limit' must be a positive integer", fn);
		}
	}

	if (auto *p = FindParam(input, "direction")) {
		result.direction = StringUtil::Lower(p->ToString());
		if (result.direction != "backward" && result.direction != "forward") {
			throw BinderException("%s 'direction' must be 'backward' or 'forward'", fn);
		}
	}

	// Default to the last hour, ending now (DESIGN.md §6.6) — never issue an unbounded query.
	int64_t now_ns = Timestamp::GetEpochNanoSeconds(Timestamp::GetCurrentTimestamp());
	result.end_ns = now_ns;
	result.start_ns = now_ns - NANOS_PER_HOUR;
	if (auto *p = FindParam(input, "start")) {
		result.start_ns = ResolveTimeBound(*p, now_ns);
		result.has_param_start = true;
	}
	if (auto *p = FindParam(input, "end")) {
		result.end_ns = ResolveTimeBound(*p, now_ns);
		result.has_param_end = true;
	}

	// `labels := ['job','level']` promotes each named label to a top-level VARCHAR column so its
	// values are directly selectable and (for loki()) its predicates are pushable. Unlisted labels
	// still appear inside the `labels` MAP. See DESIGN.md §3.2 / §4.2.
	if (auto *p = FindParam(input, "labels")) {
		for (const auto &child : ListValue::GetChildren(*p)) {
			if (child.IsNull()) {
				throw BinderException("%s 'labels' must not contain NULL entries", fn);
			}
			result.label_columns.push_back(child.ToString());
		}
	}

	// Output schema (DESIGN.md §3.2): timestamp, line, one column per promoted label,
	// then the full labels MAP and per-entry structured_metadata MAP.
	return_types.push_back(LogicalType::TIMESTAMP_NS);
	names.emplace_back("timestamp");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("line");
	for (const auto &label : result.label_columns) {
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back(label);
	}
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("labels");
	return_types.push_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("structured_metadata");
}

unique_ptr<FunctionData> LokiScanBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiScanBindData>();

	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("loki_scan requires a LogQL query string as its first argument");
	}
	result->query = loki::BuildLogQL(input.inputs[0].GetValue<string>());

	LokiResolveCommon(context, input, *result, return_types, names, "loki_scan");
	return std::move(result);
}

unique_ptr<FunctionData> LokiBind(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<LokiScanBindData>();
	result->pushdown_mode = true;

	// `selector := '{job="api"}'` is the base stream selector (DESIGN.md §3.2). It may be omitted
	// when pushdown can synthesize at least one matcher from WHERE (§4.3), so it is optional here;
	// the mandatory-selector rule is enforced at scan start once pushdown has run.
	if (auto *p = FindParam(input, "selector")) {
		result->base_selector = p->ToString();
	}

	LokiResolveCommon(context, input, *result, return_types, names, "loki");
	return std::move(result);
}

// See through the implicit cast DuckDB inserts on a column reference (e.g. when a TIMESTAMP_NS
// column is compared against a TIMESTAMPTZ constant) to reach the underlying BoundColumnRef.
const Expression &StripCast(const Expression &expr) {
	const Expression *cur = &expr;
	while (cur->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		cur = cur->Cast<BoundCastExpression>().child.get();
	}
	return *cur;
}

// If `expr` (modulo casts) is a reference to one of this scan's output columns, set `name` to that
// column's name and return true.
bool ResolveColumnName(const Expression &expr, const LogicalGet &get, std::string &name) {
	const Expression &inner = StripCast(expr);
	if (inner.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &colref = inner.Cast<BoundColumnRefExpression>();
	const auto &column_ids = get.GetColumnIds();
	idx_t proj = colref.binding.column_index;
	if (proj >= column_ids.size()) {
		return false;
	}
	idx_t storage = column_ids[proj].GetPrimaryIndex();
	if (storage >= get.names.size()) {
		return false;
	}
	name = get.names[storage];
	return true;
}

// Evaluate a constant (foldable, no column refs) sub-expression to a Value.
bool EvalConstant(ClientContext &context, const Expression &expr, Value &out) {
	if (!expr.IsFoldable()) {
		return false;
	}
	return ExpressionExecutor::TryEvaluateScalar(context, expr, out) && !out.IsNull();
}

bool IsLabelColumn(const LokiScanBindData &bind_data, const std::string &name) {
	return std::find(bind_data.label_columns.begin(), bind_data.label_columns.end(), name) !=
	       bind_data.label_columns.end();
}

// Fold a pushed timestamp lower/upper bound into the accumulated window (intersection).
void AddStartBound(LokiScanBindData &bind_data, int64_t ns) {
	bind_data.pushed_start_ns = std::max(bind_data.pushed_start_ns, ns);
}
void AddEndBound(LokiScanBindData &bind_data, int64_t ns) {
	bind_data.pushed_end_ns = std::min(bind_data.pushed_end_ns, ns);
}

// Try to translate a comparison predicate. Returns true if the predicate was fully consumed
// (may be erased); returns false to leave it as a residual — including the cases where we push a
// coarse bound to Loki but still need DuckDB to apply the exact predicate.
bool TryComparison(ClientContext &context, const LogicalGet &get, LokiScanBindData &bind_data,
                   const BoundComparisonExpression &cmp) {
	ExpressionType op = cmp.GetExpressionType();
	std::string col_name;
	Value constant;

	// Find the column side and the constant side (in either order); flip the operator if the
	// column is on the right so `op` always reads column-<op>-constant.
	if (ResolveColumnName(*cmp.left, get, col_name) && EvalConstant(context, *cmp.right, constant)) {
		// column <op> constant
	} else if (ResolveColumnName(*cmp.right, get, col_name) && EvalConstant(context, *cmp.left, constant)) {
		op = FlipComparisonExpression(op);
	} else {
		return false;
	}

	if (col_name == "timestamp") {
		// Coarse time-window bound: push it to Loki, but keep the predicate residual so DuckDB
		// applies it exactly (guards against any boundary/inclusivity mismatch). Loki's `end` is
		// exclusive, so an upper bound is nudged +1ns to keep the window a superset.
		int64_t ns = ValueToEpochNsExact(constant);
		switch (op) {
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			AddStartBound(bind_data, ns);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			AddEndBound(bind_data, ns);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			AddEndBound(bind_data, ns + 1);
			break;
		case ExpressionType::COMPARE_EQUAL:
			AddStartBound(bind_data, ns);
			AddEndBound(bind_data, ns + 1);
			break;
		default:
			break;
		}
		return false; // always residual
	}

	if (IsLabelColumn(bind_data, col_name)) {
		std::string value = constant.ToString();
		if (op == ExpressionType::COMPARE_EQUAL) {
			bind_data.pushed_matchers.push_back({col_name, loki::MatchOp::EQ, value});
			return true; // Loki `{l="v"}` matches exactly; safe to erase.
		}
		if (op == ExpressionType::COMPARE_NOTEQUAL) {
			// Loki `{l!="v"}` also matches streams *missing* the label (empty != "v"), whereas SQL
			// `l != 'v'` drops NULLs. Push it as a coarse refinement but keep it residual.
			bind_data.pushed_matchers.push_back({col_name, loki::MatchOp::NEQ, value});
			return false;
		}
	}

	// Everything else (label ordering, exact `line = ...`, unmatched columns) stays residual.
	return false;
}

// Try to translate a WHERE `col IN (a, b, ...)` on a label column into a `{l=~"a|b"}` matcher.
bool TryInList(ClientContext &context, const LogicalGet &get, LokiScanBindData &bind_data,
               const BoundOperatorExpression &op) {
	if (op.GetExpressionType() != ExpressionType::COMPARE_IN || op.children.size() < 2) {
		return false;
	}
	std::string col_name;
	if (!ResolveColumnName(*op.children[0], get, col_name) || !IsLabelColumn(bind_data, col_name)) {
		return false;
	}
	std::string regex;
	for (idx_t i = 1; i < op.children.size(); i++) {
		Value value;
		if (!EvalConstant(context, *op.children[i], value)) {
			return false; // a non-constant member — cannot translate the whole IN
		}
		if (!regex.empty()) {
			regex += "|";
		}
		regex += loki::RegexEscapeLiteral(value.ToString());
	}
	// Loki label regex matchers are fully anchored, so `{l=~"a|b"}` matches exactly {a, b}: erase.
	bind_data.pushed_matchers.push_back({col_name, loki::MatchOp::RE, regex});
	return true;
}

// Try to translate a LIKE/NOT LIKE/regexp_matches call into a line filter (only for `line`).
bool TryFunction(ClientContext &context, const LogicalGet &get, LokiScanBindData &bind_data,
                 const BoundFunctionExpression &func) {
	const std::string &fname = func.function.name;
	const bool is_like = fname == "~~";
	const bool is_not_like = fname == "!~~";
	const bool is_regexp = fname == "regexp_matches";
	if ((!is_like && !is_not_like && !is_regexp) || func.children.size() != 2) {
		return false;
	}

	std::string col_name;
	if (!ResolveColumnName(*func.children[0], get, col_name)) {
		return false;
	}
	Value pattern;
	if (!EvalConstant(context, *func.children[1], pattern)) {
		return false;
	}

	if (col_name != "line") {
		// Label LIKE/regex is not pushed in v0.4: Loki label regex is anchored (unlike SQL), and a
		// substring LIKE on a label has no direct matcher. Leave it residual.
		return false;
	}

	if (is_regexp) {
		// SQL regexp_matches and Loki `|~` are both RE2 partial matches, so this is exact: erase.
		bind_data.pushed_line_filters.push_back({loki::LineOp::MATCH, pattern.ToString()});
		return true;
	}

	// LIKE: only a pure `%substring%` pattern maps losslessly to a `|=`/`!=` substring filter.
	std::string substring;
	if (!loki::TryLikeToSubstring(pattern.ToString(), substring)) {
		return false;
	}
	bind_data.pushed_line_filters.push_back({is_like ? loki::LineOp::CONTAINS : loki::LineOp::NOT_CONTAINS, substring});
	return true;
}

// pushdown_complex_filter (DESIGN.md §4.4): walk the AND-ed WHERE predicates, translate the ones
// Loki can execute into selector matchers / line filters / time bounds, ERASE only those fully
// honored by Loki, and return the rest as residuals for DuckDB to apply.
void LokiPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                               vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<LokiScanBindData>();
	vector<unique_ptr<Expression>> residuals;
	for (auto &filter : filters) {
		bool consumed = false;
		switch (filter->GetExpressionClass()) {
		case ExpressionClass::BOUND_COMPARISON:
			consumed = TryComparison(context, get, bind_data, filter->Cast<BoundComparisonExpression>());
			break;
		case ExpressionClass::BOUND_OPERATOR:
			consumed = TryInList(context, get, bind_data, filter->Cast<BoundOperatorExpression>());
			break;
		case ExpressionClass::BOUND_FUNCTION:
			consumed = TryFunction(context, get, bind_data, filter->Cast<BoundFunctionExpression>());
			break;
		default:
			break;
		}
		if (!consumed) {
			residuals.push_back(std::move(filter));
		}
	}
	filters = std::move(residuals);
}

unique_ptr<GlobalTableFunctionState> LokiScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LokiScanBindData>();

	// Resolve the effective query and time window. For loki() the query is assembled now, after
	// pushdown has populated the matchers/line filters/bounds.
	std::string query = bind_data.query;
	int64_t start_ns = bind_data.start_ns;
	int64_t end_ns = bind_data.end_ns;
	if (bind_data.pushdown_mode) {
		query =
		    loki::BuildSelectorQuery(bind_data.base_selector, bind_data.pushed_matchers, bind_data.pushed_line_filters);
		// Mandatory-selector rule (DESIGN.md §4.3): Loki refuses a query with no matcher. An empty
		// selector renders as `{}`; require a base selector or at least one pushed label matcher.
		if (query.size() >= 2 && query[0] == '{' && query[1] == '}') {
			throw BinderException("loki: a stream selector is required — pass selector := '{job=\"api\"}' or add a "
			                      "WHERE equality/IN on a promoted label column (labels := [...])");
		}
		// Pushed WHERE bounds refine the window: they override the default when no explicit
		// start:=/end:= was given, and intersect with it when one was.
		if (bind_data.pushed_start_ns != std::numeric_limits<int64_t>::min()) {
			start_ns =
			    bind_data.has_param_start ? std::max(start_ns, bind_data.pushed_start_ns) : bind_data.pushed_start_ns;
		}
		if (bind_data.pushed_end_ns != std::numeric_limits<int64_t>::max()) {
			end_ns = bind_data.has_param_end ? std::min(end_ns, bind_data.pushed_end_ns) : bind_data.pushed_end_ns;
		}
	}

	auto headers = loki::BuildAuthHeaders(bind_data.auth);
	// Request categorized labels so structured metadata comes back per-entry (separated from the
	// indexed stream labels) instead of being folded into the stream label set. Servers that do
	// not support the flag ignore it and simply return no structured metadata (DESIGN.md §5).
	headers.emplace_back("X-Loki-Response-Encoding-Flags", "categorize-labels");
	loki::StreamPager pager(query, start_ns, end_ns, bind_data.direction, bind_data.limit);

	// No network here: the first page is fetched lazily on the first output call so that
	// binding/DESCRIBE stay cheap and offline.
	auto state = make_uniq<LokiScanGlobalState>(bind_data.endpoint, std::move(headers), std::move(pager));
	// Projection pushdown (DESIGN.md §6.3): column_ids holds the bind-schema indices to emit,
	// in output order, so we only build the requested columns (skipping the expensive MAPs).
	state->column_ids = input.column_ids;
	return std::move(state);
}

// Build a MAP(VARCHAR, VARCHAR) Value from a string map (labels or structured metadata).
Value MapFromStrings(const std::map<std::string, std::string> &m) {
	InsertionOrderPreservingMap<string> ordered;
	for (const auto &kv : m) {
		ordered.insert(kv.first, kv.second);
	}
	return Value::MAP(ordered);
}

void LokiScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<LokiScanGlobalState>();
	auto &bind_data = data_p.bind_data->Cast<LokiScanBindData>();

	// Source-column index meaning: 0=timestamp, 1=line, [2, 2+N)=promoted label column,
	// 2+N=labels MAP, 3+N=structured_metadata MAP (N = number of promoted labels).
	const idx_t num_labels = bind_data.label_columns.size();
	const column_t labels_map_col = 2 + num_labels;

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		if (state.buf_offset >= state.buffer.size()) {
			if (state.pager.Done()) {
				break;
			}
			// Fetch and decode the next page.
			auto request = state.pager.NextRequest();
			std::string query_string = loki::BuildQueryString(request);
			auto response = loki::HttpGet(state.endpoint, request.path, query_string, state.headers);
			if (!response.error.empty()) {
				throw IOException("loki_scan: request to Loki failed: " + response.error);
			}
			if (response.status < 200 || response.status >= 300) {
				throw IOException(StringUtil::Format("loki_scan: Loki returned HTTP %d: %s", response.status,
				                                     response.body.substr(0, 1000)));
			}
			// ParseStreamsResponse throws std::runtime_error on malformed/non-streams bodies.
			auto chunks = loki::ParseStreamsResponse(response.body);
			state.buffer = state.pager.Accept(chunks);
			state.buf_offset = 0;
			if (state.buffer.empty()) {
				continue; // page yielded no new rows (all de-duped, or exhausted); loop re-checks Done()
			}
		}

		const auto &row = state.buffer[state.buf_offset];
		// Projection pushdown: emit only the requested columns, in output order. output.data[out]
		// corresponds to source column state.column_ids[out]. Unrequested MAP columns are skipped.
		for (idx_t out = 0; out < state.column_ids.size(); out++) {
			const column_t src = state.column_ids[out];
			if (src == COLUMN_IDENTIFIER_ROW_ID || IsVirtualColumn(src)) {
				continue; // we emit no row-ids or virtual columns
			}
			auto &vec = output.data[out];
			if (src == 0) {
				// TIMESTAMP_NS is stored as raw nanoseconds (do NOT use Timestamp::FromEpochNanoSeconds,
				// which rescales to microseconds for the µs TIMESTAMP type).
				FlatVector::GetData<int64_t>(vec)[count] = row.ts_ns;
			} else if (src == 1) {
				vec.SetValue(count, Value(row.line));
			} else if (src < labels_map_col) {
				// A promoted label column: its value, or NULL where the stream lacks the label.
				const auto &name = bind_data.label_columns[src - 2];
				auto it = row.labels.find(name);
				vec.SetValue(count, it != row.labels.end() ? Value(it->second) : Value(LogicalType::VARCHAR));
			} else if (src == labels_map_col) {
				vec.SetValue(count, MapFromStrings(row.labels));
			} else { // smeta_map_col
				vec.SetValue(count, MapFromStrings(row.structured_metadata));
			}
		}

		state.buf_offset++;
		count++;
	}

	output.SetCardinality(count);
}

// Named parameters shared by loki_scan and loki (connection/auth, time window, paging, labels).
void AddCommonNamedParameters(TableFunction &fn) {
	fn.named_parameters["endpoint"] = LogicalType::VARCHAR;
	fn.named_parameters["secret"] = LogicalType::VARCHAR;
	fn.named_parameters["token"] = LogicalType::VARCHAR;
	fn.named_parameters["username"] = LogicalType::VARCHAR;
	fn.named_parameters["password"] = LogicalType::VARCHAR;
	fn.named_parameters["tenant"] = LogicalType::VARCHAR;
	fn.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	// start/end accept TIMESTAMP, TIMESTAMPTZ, or INTERVAL (offset from now); typed ANY and
	// dispatched at bind time.
	fn.named_parameters["start"] = LogicalType::ANY;
	fn.named_parameters["end"] = LogicalType::ANY;
	fn.named_parameters["limit"] = LogicalType::BIGINT;
	fn.named_parameters["direction"] = LogicalType::VARCHAR;
	// labels := ['job','level'] promotes labels to top-level VARCHAR columns (DESIGN.md §3.2).
	fn.named_parameters["labels"] = LogicalType::LIST(LogicalType::VARCHAR);
	// Projection pushdown (DESIGN.md §6.3): only build the columns the query selects.
	fn.projection_pushdown = true;
}

} // namespace

void RegisterLokiScanFunction(ExtensionLoader &loader) {
	// loki_scan(query, ...): raw LogQL, no predicate translation (DESIGN.md §3.3).
	TableFunction loki_scan("loki_scan", {LogicalType::VARCHAR}, LokiScanFunction, LokiScanBind, LokiScanInitGlobal);
	AddCommonNamedParameters(loki_scan);
	loader.RegisterFunction(loki_scan);

	// loki(selector := ..., labels := [...], ...): pushdown-driven (DESIGN.md §3.2, §4). All
	// arguments are named; WHERE predicates on promoted labels / timestamp / line are translated
	// into the LogQL selector, time bounds, and line filters via pushdown_complex_filter.
	TableFunction loki("loki", {}, LokiScanFunction, LokiBind, LokiScanInitGlobal);
	AddCommonNamedParameters(loki);
	loki.named_parameters["selector"] = LogicalType::VARCHAR;
	loki.pushdown_complex_filter = LokiPushdownComplexFilter;
	loader.RegisterFunction(loki);
}

} // namespace duckdb
