#include "postgres_connector.h"
#include <iostream>
#include <stdexcept>
#include <unordered_map>

PostgresConnector::PostgresConnector() {
    connection_ = nullptr;
}

PostgresConnector::~PostgresConnector() {
    disconnect();
}

bool PostgresConnector::connect(const std::string& conninfo) {
    connection_ = PQconnectdb(conninfo.c_str());
    return PQstatus(connection_) == CONNECTION_OK;
}

void PostgresConnector::disconnect() {
    if (connection_ != nullptr) {
        PQfinish(connection_);
        connection_ = nullptr;
    }
}

bool PostgresConnector::is_connected() const {
    return connection_ != nullptr && PQstatus(connection_) == CONNECTION_OK;
}

std::string PostgresConnector::oid_to_type_name(Oid type_oid) const {
    static const std::unordered_map<Oid, std::string> type_map = {
        // Числовые типы
        {16, "bool"},           // boolean
        {17, "bytea"},          // byte array
        {18, "char"},           // character
        {20, "int8"},           // bigint
        {21, "int2"},           // smallint
        {23, "int4"},           // integer
        {26, "oid"},            // object identifier
        {700, "float4"},        // real
        {701, "float8"},        // double precision
        {1700, "numeric"},      // numeric/decimal
        
        // Строковые типы
        {25, "text"},           // text
        {1043, "varchar"},      // character varying
        {1042, "bpchar"},       // character
        {19, "name"},           // name
        {2275, "cstring"},      // cstring
        
        // Дата и время
        {1082, "date"},         // date
        {1083, "time"},         // time
        {1114, "timestamp"},    // timestamp without timezone
        {1184, "timestamptz"},  // timestamp with timezone
        {1186, "interval"},     // interval
        {1266, "timetz"},       // time with timezone
        
        // JSON
        {114, "json"},          // json
        {3802, "jsonb"},        // json binary
        
        // Массивы
        {1000, "bool[]"},       // boolean array
        {1005, "int2[]"},       // smallint array
        {1007, "int4[]"},       // integer array
        {1016, "int8[]"},       // bigint array
        {1021, "float4[]"},     // real array
        {1022, "float8[]"},     // double precision array
        {1009, "text[]"},       // text array
        {1015, "varchar[]"},    // varchar array
        {1231, "numeric[]"},    // numeric array
        
        // Геометрические типы
        {600, "point"},         // point
        {601, "line"},          // line
        {602, "lseg"},         // line segment
        {603, "box"},          // box
        {604, "path"},         // path
        {628, "line"},         // line (another)
        
        // Сетевые адреса
        {869, "inet"},          // inet
        {650, "cidr"},          // cidr
        {1040, "macaddr"},      // macaddr
        {1041, "macaddr8"},     // macaddr8
        
        // XML
        {142, "xml"},           // xml
        
        // UUID
        {2950, "uuid"},         // uuid
        
        // Двоичные данные
        {2278, "bytea"},        // byte array
    };
    
    auto it = type_map.find(type_oid);
    if (it != type_map.end()) {
        return it->second;
    }
    
    if (is_connected()) {
        std::string query = "SELECT typname FROM pg_type WHERE oid = " + std::to_string(type_oid);
        PGresult* res = PQexec(connection_, query.c_str());
        
        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            std::string type_name = PQgetvalue(res, 0, 0);
            PQclear(res);
            return type_name;
        }
        PQclear(res);
    }
    
    return "oid_" + std::to_string(type_oid);
}

QueryResult PostgresConnector::execute(const std::string& query) {
    QueryResult result;
    
    if (!is_connected()) {
        throw std::runtime_error("Not connected to PostgreSQL");
    }

    PGresult* res = PQexec(connection_, query.c_str());
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::string error = PQresultErrorMessage(res);
        PQclear(res);
        throw std::runtime_error("Query failed: " + error);
    }

    int num_cols = PQnfields(res);
    for (int i = 0; i < num_cols; ++i) {
        ColumnInfo col;
        col.name = PQfname(res, i);
        
        Oid type_oid = PQftype(res, i);
        col.type = oid_to_type_name(type_oid);
        
        result.columns.push_back(col);
    }

    int num_rows = PQntuples(res);
    for (int i = 0; i < num_rows; ++i) {
        std::vector<std::string> row;
        for (int j = 0; j < num_cols; ++j) {
            char* value = PQgetvalue(res, i, j);
            if (value != nullptr) {
                row.push_back(std::string(value));
            } else {
                row.push_back("NULL");
            }
        }
        result.rows.push_back(row);
    }

    PQclear(res);
    return result;
}

std::string PostgresConnector::execute_to_json(const std::string& query) {
    QueryResult result = execute(query);
    return result.to_json();
}