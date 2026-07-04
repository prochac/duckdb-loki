#include "loki/parse.hpp"

#include <stdexcept>
#include <string>

#include <yyjson.h>

namespace duckdb {
namespace loki {

namespace {

// Truncated view of a response body, for embedding in error messages.
std::string Snippet(const std::string &body) {
	static constexpr size_t MAX = 500;
	return body.size() > MAX ? body.substr(0, MAX) + "..." : body;
}

// RAII guard so the yyjson document is freed on every exit path.
struct DocGuard {
	yyjson_doc *doc;
	~DocGuard() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
};

} // namespace

std::vector<std::string> ParseStringArrayResponse(const std::string &json) {
	yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
	DocGuard guard {doc};
	if (!doc) {
		throw std::runtime_error("Loki response is not valid JSON: " + Snippet(json));
	}

	yyjson_val *data = yyjson_obj_get(yyjson_doc_get_root(doc), "data");
	if (!data || !yyjson_is_arr(data)) {
		// A non-2xx Loki error usually arrives as a bare message or lacks the array; surface it.
		throw std::runtime_error("Loki response missing array 'data' field: " + Snippet(json));
	}

	std::vector<std::string> values;
	size_t idx, max;
	yyjson_val *elem;
	yyjson_arr_foreach(data, idx, max, elem) {
		const char *s = yyjson_get_str(elem);
		if (s) {
			values.emplace_back(s);
		}
	}
	return values;
}

std::vector<std::map<std::string, std::string>> ParseSeriesResponse(const std::string &json) {
	yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
	DocGuard guard {doc};
	if (!doc) {
		throw std::runtime_error("Loki response is not valid JSON: " + Snippet(json));
	}

	yyjson_val *data = yyjson_obj_get(yyjson_doc_get_root(doc), "data");
	if (!data || !yyjson_is_arr(data)) {
		throw std::runtime_error("Loki response missing array 'data' field: " + Snippet(json));
	}

	std::vector<std::map<std::string, std::string>> series;
	size_t idx, max;
	yyjson_val *elem;
	yyjson_arr_foreach(data, idx, max, elem) {
		std::map<std::string, std::string> labels;
		yyjson_obj_iter iter;
		yyjson_obj_iter_init(elem, &iter);
		yyjson_val *key;
		while ((key = yyjson_obj_iter_next(&iter))) {
			const char *k = yyjson_get_str(key);
			const char *v = yyjson_get_str(yyjson_obj_iter_get_val(key));
			if (k && v) {
				labels.emplace(k, v);
			}
		}
		series.push_back(std::move(labels));
	}
	return series;
}

std::vector<StreamChunk> ParseStreamsResponse(const std::string &json) {
	yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
	DocGuard guard {doc};
	if (!doc) {
		throw std::runtime_error("Loki response is not valid JSON: " + Snippet(json));
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *data = yyjson_obj_get(root, "data");
	if (!data) {
		// A non-2xx Loki error usually arrives as a bare message; surface it verbatim.
		throw std::runtime_error("Loki response missing 'data' field: " + Snippet(json));
	}

	yyjson_val *result_type = yyjson_obj_get(data, "resultType");
	const char *rt = yyjson_get_str(result_type);
	if (!rt || std::string(rt) != "streams") {
		throw std::runtime_error("Loki resultType is '" + std::string(rt ? rt : "(none)") +
		                         "', expected 'streams'; loki_scan supports log queries only (metric "
		                         "queries are not supported yet)");
	}

	std::vector<StreamChunk> chunks;
	yyjson_val *result = yyjson_obj_get(data, "result");
	size_t idx, max;
	yyjson_val *elem;
	yyjson_arr_foreach(result, idx, max, elem) {
		StreamChunk chunk;

		yyjson_val *stream = yyjson_obj_get(elem, "stream");
		if (stream) {
			yyjson_obj_iter iter;
			yyjson_obj_iter_init(stream, &iter);
			yyjson_val *key;
			while ((key = yyjson_obj_iter_next(&iter))) {
				const char *k = yyjson_get_str(key);
				const char *v = yyjson_get_str(yyjson_obj_iter_get_val(key));
				if (k && v) {
					chunk.labels.emplace(k, v);
				}
			}
		}

		yyjson_val *values = yyjson_obj_get(elem, "values");
		size_t vidx, vmax;
		yyjson_val *pair;
		yyjson_arr_foreach(values, vidx, vmax, pair) {
			const char *ts_str = yyjson_get_str(yyjson_arr_get(pair, 0));
			const char *line_str = yyjson_get_str(yyjson_arr_get(pair, 1));
			if (!ts_str) {
				continue;
			}
			int64_t ns;
			try {
				ns = std::stoll(ts_str);
			} catch (const std::exception &) {
				continue; // skip an unparseable timestamp rather than fail the whole scan
			}
			StreamEntry entry;
			entry.ts_ns = ns;
			entry.line = line_str ? std::string(line_str) : std::string();
			// With the `categorize-labels` response flag (which loki_scan always requests), the
			// optional 3rd element is an object like {"structuredMetadata": {...}, "parsed": {...}}.
			// Extract structuredMetadata; entries without any get an empty {} here. (Without the
			// flag Loki folds structured metadata into the stream labels instead, and this is
			// simply absent — the map stays empty.) `parsed` pipeline labels are not surfaced yet.
			yyjson_val *categorized = yyjson_arr_get(pair, 2);
			yyjson_val *sm = yyjson_obj_get(categorized, "structuredMetadata");
			if (sm) {
				yyjson_obj_iter sm_iter;
				yyjson_obj_iter_init(sm, &sm_iter);
				yyjson_val *sm_key;
				while ((sm_key = yyjson_obj_iter_next(&sm_iter))) {
					const char *k = yyjson_get_str(sm_key);
					const char *v = yyjson_get_str(yyjson_obj_iter_get_val(sm_key));
					if (k && v) {
						entry.structured_metadata.emplace(k, v);
					}
				}
			}
			chunk.values.push_back(std::move(entry));
		}

		chunks.push_back(std::move(chunk));
	}

	return chunks;
}

} // namespace loki
} // namespace duckdb
