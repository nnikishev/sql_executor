#include "clickhouse_connector.h"
#include <clickhouse/client.h>
#include <clickhouse/columns/column.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/block.h>
#include <iostream>
#include <stdexcept>

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

QueryResult ClickHouseConnector::execute(const std::string& query) {
    QueryResult result;
    
    if (!is_connected()) {
        throw std::runtime_error("Not connected to ClickHouse");
    }

    try {
        client_->Select(query, [&result, this](const Block& block) {
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
            size_t col_count = block.GetColumnCount();
            
            for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
                std::vector<std::string> row;
                
                for (size_t col_idx = 0; col_idx < col_count; ++col_idx) {
                    auto column = block[col_idx];
                    std::string value;
                    
                    
                    if (auto col = column->As<ColumnString>()) {
                        value = col->At(row_idx);
                    }
                    else if (auto col = column->As<ColumnFixedString>()) {
                        value = col->At(row_idx);
                    }
                    else if (auto col = column->As<ColumnInt8>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnInt16>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnInt32>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnInt64>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnUInt8>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnUInt16>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnUInt32>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnUInt64>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnFloat32>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnFloat64>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnDate>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else if (auto col = column->As<ColumnDateTime>()) {
                        value = std::to_string(col->At(row_idx));
                    }
                    else {
                        try {
                            if (auto col_str = column->As<ColumnString>()) {
                                value = col_str->At(row_idx);
                            } else {
                                value = "UNSUPPORTED_TYPE";
                            }
                        } catch (...) {
                            value = "UNSUPPORTED_TYPE";
                        }
                    }
                    
                    row.push_back(value);
                }
                
                result.rows.push_back(row);
            }
        });
        
    } catch (const std::exception& e) {
        throw std::runtime_error("ClickHouse query failed: " + std::string(e.what()));
    }
    
    return result;
}

std::string ClickHouseConnector::execute_to_json(const std::string& query) {
    QueryResult result = execute(query);
    return result.to_json();
}