#ifndef POSTGRES_CONNECTOR_H
#define POSTGRES_CONNECTOR_H

#include <string>
#include <vector>
#include <libpq-fe.h>
#include "common.h" 

class PostgresConnector {
private:
    PGconn* connection_;

public:
    PostgresConnector();
    ~PostgresConnector();
    
    bool connect(const std::string& conninfo);
    void disconnect();
    bool is_connected() const;
    QueryResult execute(const std::string& query);
    std::string execute_to_json(const std::string& query);

private:
    std::string oid_to_type_name(Oid type_oid) const;
};

#endif