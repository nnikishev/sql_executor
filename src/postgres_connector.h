#ifndef POSTGRES_CONNECTOR_H
#define POSTGRES_CONNECTOR_H

#include <string>
#include <vector>
#include <libpq-fe.h>
#include "common.h" 

class PostgresConnector {
private:
    PGconn* connection_;
    bool in_transaction_;  

public:
    PostgresConnector();
    ~PostgresConnector();
    
    bool connect(const std::string& conninfo);
    void disconnect();
    bool is_connected() const;
    
    QueryResult execute(const std::string& query);
    std::string execute_to_json(const std::string& query);
    
    bool begin_transaction();
    bool commit_transaction();
    bool rollback_transaction();
    bool is_in_transaction() const;
    
    bool execute_batch(const std::vector<std::string>& queries);

private:
    std::string oid_to_type_name(Oid type_oid) const;
    bool execute_simple_query(const std::string& query);
};

#endif