#include "clickhouse_connector.h"
#include <clickhouse/client.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/block.h>
#include <iostream>
#include <stdexcept>
#include <sstream>

using namespace clickhouse;

ClickHouseConnector::ClickHouseConnector() : client_(nullptr) {}

ClickHouseConnector::~ClickHouseConnector() {
    disconnect();
}

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

std::string ClickHouseConnector::normalize_type_name(const std::string& type_name) const {
    std::string result = type_name;
    
    if (result.find("Nullable(") != std::string::npos) {
        size_t start = result.find('(') + 1;
        size_t end = result.rfind(')');
        if (start != std::string::npos && end != std::string::npos) {
            result = result.substr(start, end - start) + "?";
        }
    }
    
    if (result.find("LowCardinality(") != std::string::npos) {
        size_t start = result.find('(') + 1;
        size_t end = result.rfind(')');
        if (start != std::string::npos && end != std::string::npos) {
            result = result.substr(start, end - start);
        }
    }
    
    return result;
}

std::string value_to_string(const ColumnRef& column, size_t row_idx) {
    
    if (auto col = column->As<ColumnString>()) {
        std::string_view sv = col->At(row_idx);
        return std::string(sv);
    }
    else if (auto col = column->As<ColumnFixedString>()) {
        std::string_view sv = col->At(row_idx);
        return std::string(sv);
    }
    else if (auto col = column->As<ColumnInt8>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnInt16>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnInt32>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnInt64>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnUInt8>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnUInt16>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnUInt32>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnUInt64>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnFloat32>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnFloat64>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnDate>()) {
        return std::to_string(col->At(row_idx));
    }
    else if (auto col = column->As<ColumnDateTime>()) {
        return std::to_string(col->At(row_idx));
    }
    else {
        return "[" + column->Type()->GetName() + "]";
    }
}

QueryResult ClickHouseConnector::execute(const std::string& query) {
    QueryResult result;
    
    if (!is_connected()) {
        throw std::runtime_error("Not connected to ClickHouse");
    }

    std::string count_query;
    
    if (query.find("SELECT") != std::string::npos && 
        query.find("COUNT(") == std::string::npos) {
        
        size_t from_pos = query.find("FROM");
        if (from_pos != std::string::npos) {
            count_query = "SELECT COUNT(*) " + query.substr(from_pos);
            
            // Убираем ORDER BY, LIMIT для корректного подсчета
            size_t order_pos = count_query.find("ORDER BY");
            if (order_pos != std::string::npos) {
                count_query = count_query.substr(0, order_pos);
            }
            
            size_t limit_pos = count_query.find("LIMIT");
            if (limit_pos != std::string::npos) {
                count_query = count_query.substr(0, limit_pos);
            }
        }
    }
    
    if (!count_query.empty()) {
        try {
            client_->Select(count_query, [&result](const Block& block) {
                if (block.GetRowCount() > 0 && block.GetColumnCount() > 0) {
                    auto count_column = block[0];
                    if (auto col = count_column->As<ColumnUInt64>()) {
                        result.count = col->At(0);
                    }
                }
            });
        } catch (...) {
            result.count = 0;
        }
    }

    try {
        size_t total_rows = 0;
        
        client_->Select(query, [&result, &total_rows, this](const Block& block) {
            if (result.columns.empty()) {
                for (size_t i = 0; i < block.GetColumnCount(); ++i) {
                    ColumnInfo col;
                    col.name = block.GetColumnName(i);
                    
                    auto column = block[i];
                    col.type = normalize_type_name(column->Type()->GetName());
                    
                    result.columns.push_back(col);
                }
            }

            size_t row_count = block.GetRowCount();
            total_rows += row_count;
            
            for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
                std::vector<std::string> row;
                
                for (size_t col_idx = 0; col_idx < block.GetColumnCount(); ++col_idx) {
                    auto column = block[col_idx];
                    std::string value = value_to_string(column, row_idx);
                    row.push_back(value);
                }
                
                result.rows.push_back(row);
            }
        });
        
        if (result.count == 0) {
            result.count = total_rows;
        }
        
    } catch (const std::exception& e) {
        throw std::runtime_error("ClickHouse query failed: " + std::string(e.what()));
    }
    
    return result;
}

std::string ClickHouseConnector::execute_to_json(const std::string& query) {
    QueryResult result = execute(query);
    return result.to_json();
}