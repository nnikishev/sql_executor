#include "clickhouse_connector.h"
#include <clickhouse/client.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/block.h>
#include <inttypes.h>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <cstdio>
#include <algorithm>
#include <cctype>

using namespace clickhouse;

ClickHouseConnector::ClickHouseConnector() : client_(nullptr) {}
ClickHouseConnector::~ClickHouseConnector() { disconnect(); }

bool ClickHouseConnector::connect(const std::string& host, int port,
                                  const std::string& database, const std::string& user,
                                  const std::string& password) {
    try {
        ClientOptions options;
        options.SetHost(host);
        options.SetPort(port);
        options.SetDefaultDatabase(database);
        options.SetUser(user);
        options.SetPassword(password);

        client_ = std::make_unique<Client>(options);
        client_->Execute("SELECT 1");
        return true;
    } catch (const std::exception& e) {
        std::cerr << "ClickHouse connection error: " << e.what() << std::endl;
        client_.reset();
        return false;
    }
}

void ClickHouseConnector::disconnect() {
    client_.reset();
}

bool ClickHouseConnector::is_connected() const {
    return client_ != nullptr;
}

// -------------------------
// Вспомогательные функции
// -------------------------

inline std::string to_upper(const std::string& str) {
    std::string upper = str;
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return upper;
}

std::string ClickHouseConnector::normalize_type_name(const std::string& type_name) const {
    std::string result = type_name;

    auto remove_wrapper = [&](const std::string& wrapper){
        size_t pos = result.find(wrapper + "(");
        if (pos != std::string::npos) {
            size_t start = pos + wrapper.size() + 1;
            size_t end = result.rfind(')');
            if (end != std::string::npos && end > start)
                result = result.substr(start, end - start);
        }
    };

    remove_wrapper("Nullable");
    remove_wrapper("LowCardinality");

    return result;
}

static Value value_to_variant(const ColumnRef& column, size_t row_idx) {
    try {
        // Обрабатываем Nullable типы
        if (auto nullable_col = column->As<ColumnNullable>()) {
            if (nullable_col->IsNull(row_idx)) return nullptr;
            return value_to_variant(nullable_col->Nested(), row_idx);
        }

        // Обрабатываем String
        if (auto col = column->As<ColumnString>()) return std::string(col->At(row_idx));
        if (auto col = column->As<ColumnFixedString>()) return std::string(col->At(row_idx));

        // Обрабатываем Integers
        if (auto col = column->As<ColumnInt8>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnInt16>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnInt32>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnInt64>()) return col->At(row_idx);

        if (auto col = column->As<ColumnUInt8>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnUInt16>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnUInt32>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnUInt64>()) return static_cast<int64_t>(col->At(row_idx));

        // Обрабатываем Floats
        if (auto col = column->As<ColumnFloat32>()) return static_cast<double>(col->At(row_idx));
        if (auto col = column->As<ColumnFloat64>()) return col->At(row_idx);

        // Обрабатываем Date / DateTime
        if (auto col = column->As<ColumnDate>()) return static_cast<int64_t>(col->At(row_idx));
        if (auto col = column->As<ColumnDateTime>()) return static_cast<int64_t>(col->At(row_idx));

        // Обрабатываем UUID
        if (auto col = column->As<ColumnUUID>()) {
            auto uuid = col->At(row_idx);
            char buf[37];
            snprintf(buf, sizeof(buf),
                     "%08" PRIx64 "-%04" PRIx64 "-%04" PRIx64 "-%04" PRIx64 "-%012" PRIx64,
                     (uuid.first >> 32) & 0xFFFFFFFF,
                     (uuid.first >> 16) & 0xFFFF,
                     uuid.first & 0xFFFF,
                     (uuid.second >> 48) & 0xFFFF,
                     uuid.second & 0xFFFFFFFFFFFF);
            return std::string(buf);
        }

        return "[" + column->Type()->GetName() + "]";

    } catch (const std::exception& e) {
        return "[ERROR: " + std::string(e.what()) + "]";
    }
}

// -------------------------
// Основные методы
// -------------------------

QueryResult ClickHouseConnector::execute(const std::string& query) {
    if (!is_connected()) throw std::runtime_error("Not connected to ClickHouse");

    QueryResult result;
    std::string upper_query = to_upper(query);
    std::string count_query;

    if (upper_query.find("SELECT") != std::string::npos &&
        upper_query.find("COUNT(") == std::string::npos) {
        size_t from_pos = upper_query.find("FROM");
        if (from_pos != std::string::npos) {
            count_query = "SELECT COUNT(*) " + query.substr(from_pos);

            size_t order_pos = count_query.find("ORDER BY");
            if (order_pos != std::string::npos) count_query = count_query.substr(0, order_pos);

            size_t limit_pos = count_query.find("LIMIT");
            if (limit_pos != std::string::npos) count_query = count_query.substr(0, limit_pos);
        }
    }

    // COUNT(*)
    if (!count_query.empty()) {
        try {
            client_->Select(count_query, [&result](const Block& block) {
                if (block.GetRowCount() > 0 && block.GetColumnCount() > 0) {
                    if (auto col = block[0]->As<ColumnUInt64>()) result.count = col->At(0);
                }
            });
        } catch (...) { result.count = 0; }
    }

    // Основной запрос
    size_t total_rows = 0;
    try {
        client_->Select(query, [this, &result, &total_rows](const Block& block) {
            if (result.columns.empty()) {
                for (size_t i = 0; i < block.GetColumnCount(); ++i) {
                    ColumnInfo col;
                    col.name = block.GetColumnName(i);
                    col.type = normalize_type_name(block[i]->Type()->GetName());
                    result.columns.push_back(col);
                }
            }

            size_t rows_in_block = block.GetRowCount();
            total_rows += rows_in_block;

            for (size_t row_idx = 0; row_idx < rows_in_block; ++row_idx) {
                std::vector<Value> row;
                for (size_t col_idx = 0; col_idx < block.GetColumnCount(); ++col_idx) {
                    row.push_back(value_to_variant(block[col_idx], row_idx));
                }
                result.rows.push_back(std::move(row));
            }
        });

        if (result.count == 0) result.count = total_rows;

    } catch (const std::exception& e) {
        throw std::runtime_error("ClickHouse query failed: " + std::string(e.what()));
    }

    return result;
}

std::string ClickHouseConnector::execute_to_json(const std::string& query) {
    QueryResult result = execute(query);
    return result.to_json();
}