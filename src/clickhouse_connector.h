#ifndef CLICKHOUSE_CONNECTOR_H
#define CLICKHOUSE_CONNECTOR_H

#include <string>
#include <vector>
#include <memory>
#include "common.h"

namespace clickhouse {
    class Client;
    class ClientOptions;
    class ColumnNullable; 
    class ColumnUUID;     
}

class ClickHouseConnector {
private:
    std::unique_ptr<clickhouse::Client> client_;

public:
    ClickHouseConnector();
    ~ClickHouseConnector();
    
    bool connect(const std::string& host, int port, 
                 const std::string& database, const std::string& user, 
                 const std::string& password);
    void disconnect();
    bool is_connected() const;
    QueryResult execute(const std::string& query);
    std::string execute_to_json(const std::string& query);

private:
    std::string normalize_type_name(const std::string& type_name) const;
};

#endif