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
		out.emplace_back(kv[0].ToString(), kv[1].ToString());
	}
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
	return Timestamp::GetEpochNanoSeconds(ts);
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
		try {
			return now_ns + loki::IntervalToNanos(iv.months, iv.days, iv.micros);
		} catch (const std::exception &e) {
			throw BinderException(e.what());
		}
	}
	return ValueToEpochNs(value);
}

void ResolveLokiConnection(ClientContext &context, TableFunctionBindInput &input, std::string &endpoint,
                           loki::AuthConfig &auth, const char *fn) {
	std::vector<std::pair<std::string, std::string>> secret_headers;

	// Resolve connection config: a `loki` secret, refined by inline overrides.
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (auto *secret_param = FindNamedParam(input, "secret")) {
		// Explicit secret name — must exist.
		auto name = secret_param->ToString();
		auto entry = secret_manager.GetSecretByName(transaction, name);
		if (!entry) {
			throw BinderException("%s: no secret named '%s' found", fn, name);
		}
		ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(*entry->secret), endpoint, auth, secret_headers);
	} else {
		// No explicit secret: fall back to the best-matching `loki` secret. DuckDB names an
		// unnamed CREATE SECRET (TYPE loki, ...) `__default_loki`, and empty-scope secrets match
		// any path at the lowest score, tie-breaking toward the default (leading underscore sorts first).
		auto match = secret_manager.LookupSecret(transaction, "", "loki");
		if (match.HasMatch()) {
			ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(match.GetSecret()), endpoint, auth, secret_headers);
		}
	}

	// Inline overrides take precedence over the secret.
	if (auto *p = FindNamedParam(input, "endpoint")) {
		endpoint = p->ToString();
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

	// An http(s):// path is the endpoint; any other non-empty path is a secret name (unless a
	// `secret` option overrides it). This mirrors the two ATTACH forms in DESIGN.md §3.6.
	std::string lower_path = StringUtil::Lower(path);
	bool path_is_url = StringUtil::StartsWith(lower_path, "http://") || StringUtil::StartsWith(lower_path, "https://");
	std::string secret_name;
	if (auto *p = opt("secret")) {
		secret_name = p->ToString();
	} else if (!path_is_url && !path.empty()) {
		secret_name = path;
	}

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (!secret_name.empty()) {
		auto entry = secret_manager.GetSecretByName(transaction, secret_name);
		if (!entry) {
			throw BinderException("%s: no secret named '%s' found", fn, secret_name);
		}
		ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(*entry->secret), endpoint, auth, secret_headers);
	} else {
		// No secret name and an endpoint URL (or nothing): still honor a default `loki` secret for
		// auth/tenant; the inline URL below overrides its endpoint. (Plain declaration + if, not a
		// C++17 if-init statement — DuckDB's extension build is C++11 and CI rejects the extension.)
		auto match = secret_manager.LookupSecret(transaction, "", "loki");
		if (match.HasMatch()) {
			ReadLokiSecret(dynamic_cast<const KeyValueSecret &>(match.GetSecret()), endpoint, auth, secret_headers);
		}
	}

	// Inline overrides take precedence over the secret (a URL path counts as an endpoint override).
	if (path_is_url) {
		endpoint = path;
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
		throw BinderException("%s requires an endpoint: ATTACH an http(s):// URL, pass ENDPOINT, or reference a "
		                      "loki secret (SECRET 'my_loki', or a default CREATE SECRET (TYPE loki, ENDPOINT ...))",
		                      fn);
	}
	while (!endpoint.empty() && endpoint.back() == '/') {
		endpoint.pop_back();
	}
}

} // namespace duckdb
