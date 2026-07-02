#include "loki/secret.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

namespace {

// The `config` provider: build a KeyValueSecret from the user-supplied CREATE SECRET
// options. Mirrors the built-in "http" secret in duckdb/src/main/secret/default_secrets.cpp.
unique_ptr<BaseSecret> CreateLokiSecretFromConfig(ClientContext &, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	// TrySetValue copies input.options[key] into the secret only if the user provided it,
	// so absent parameters stay unset (and later resolve to "").
	secret->TrySetValue("endpoint", input);
	secret->TrySetValue("token", input);
	secret->TrySetValue("username", input);
	secret->TrySetValue("password", input);
	secret->TrySetValue("tenant", input);
	secret->TrySetValue("headers", input);

	secret->redact_keys = {"token", "password"};
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

	CreateSecretFunction config_fun;
	config_fun.secret_type = "loki";
	config_fun.provider = "config";
	config_fun.function = CreateLokiSecretFromConfig;
	config_fun.named_parameters["endpoint"] = LogicalType::VARCHAR;
	config_fun.named_parameters["token"] = LogicalType::VARCHAR;
	config_fun.named_parameters["username"] = LogicalType::VARCHAR;
	config_fun.named_parameters["password"] = LogicalType::VARCHAR;
	config_fun.named_parameters["tenant"] = LogicalType::VARCHAR;
	config_fun.named_parameters["headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	loader.RegisterFunction(config_fun);
}

} // namespace duckdb
