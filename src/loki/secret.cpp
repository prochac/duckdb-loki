#include "loki/secret.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <cstdlib>

namespace duckdb {

namespace {

// Look up an environment variable, trying the given (upper-case) name verbatim.
const char *TryGetEnv(const char *name) {
	return std::getenv(name);
}

// Declare the `loki` secret's fields on a CreateSecretFunction. Shared by both providers so
// the `config` and `env` variants accept the same inline overrides.
void AddLokiSecretParameters(CreateSecretFunction &fun) {
	fun.named_parameters["endpoint"] = LogicalType::VARCHAR;
	fun.named_parameters["token"] = LogicalType::VARCHAR;
	fun.named_parameters["username"] = LogicalType::VARCHAR;
	fun.named_parameters["password"] = LogicalType::VARCHAR;
	fun.named_parameters["tenant"] = LogicalType::VARCHAR;
	fun.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
}

// Copy the user-supplied CREATE SECRET options into the secret. TrySetValue copies
// input.options[key] only if the user provided it, so absent parameters stay unset (and
// later resolve to ""). Inline options always take precedence over any env-seeded values.
void ApplyLokiSecretOptions(KeyValueSecret &secret, CreateSecretInput &input) {
	secret.TrySetValue("endpoint", input);
	secret.TrySetValue("token", input);
	secret.TrySetValue("username", input);
	secret.TrySetValue("password", input);
	secret.TrySetValue("tenant", input);
	secret.TrySetValue("headers", input);
	secret.redact_keys = {"token", "password"};
}

// The `config` provider: build a KeyValueSecret purely from the CREATE SECRET options.
// Mirrors the built-in "http" secret in duckdb/src/main/secret/default_secrets.cpp.
unique_ptr<BaseSecret> CreateLokiSecretFromConfig(ClientContext &, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);
	ApplyLokiSecretOptions(*secret, input);
	return std::move(secret);
}

// The `env` provider: seed the secret from the standard logcli environment variables
// (the Loki analog of libpq's PG* vars), then let inline CREATE SECRET options override.
//   LOKI_ADDR         -> endpoint
//   LOKI_BEARER_TOKEN -> token
//   LOKI_USERNAME     -> username
//   LOKI_PASSWORD     -> password
//   LOKI_ORG_ID       -> tenant (X-Scope-OrgID)
unique_ptr<BaseSecret> CreateLokiSecretFromEnv(ClientContext &, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	if (auto *addr = TryGetEnv("LOKI_ADDR")) {
		secret->secret_map["endpoint"] = Value(addr);
	}
	if (auto *token = TryGetEnv("LOKI_BEARER_TOKEN")) {
		secret->secret_map["token"] = Value(token);
	}
	if (auto *username = TryGetEnv("LOKI_USERNAME")) {
		secret->secret_map["username"] = Value(username);
	}
	if (auto *password = TryGetEnv("LOKI_PASSWORD")) {
		secret->secret_map["password"] = Value(password);
	}
	if (auto *org_id = TryGetEnv("LOKI_ORG_ID")) {
		secret->secret_map["tenant"] = Value(org_id);
	}

	// Inline options override the environment-seeded values.
	ApplyLokiSecretOptions(*secret, input);
	return std::move(secret);
}

} // namespace

void RegisterLokiSecretType(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "loki";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "loki";
	loader.RegisterSecretType(secret_type);

	// config provider: CREATE SECRET (TYPE loki, ENDPOINT ..., TOKEN ..., ...)
	CreateSecretFunction config_fun;
	config_fun.secret_type = "loki";
	config_fun.provider = "config";
	config_fun.function = CreateLokiSecretFromConfig;
	AddLokiSecretParameters(config_fun);
	loader.RegisterFunction(config_fun);

	// env provider: CREATE SECRET (TYPE loki, PROVIDER env) — reads LOKI_* from the environment.
	CreateSecretFunction env_fun;
	env_fun.secret_type = "loki";
	env_fun.provider = "env";
	env_fun.function = CreateLokiSecretFromEnv;
	AddLokiSecretParameters(env_fun);
	loader.RegisterFunction(env_fun);
}

} // namespace duckdb
