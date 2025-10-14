#include "postgres_connector.h"
#include "common.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>

TEST_CASE("Postgres connection", "[PostgresConnector]") {
    PostgresConnector conn;
    REQUIRE_FALSE(conn.is_connected());

    std::string conninfo = "host=127.0.0.1 port=15432 dbname=postgres user=postgres password=postgres";
    if (conn.connect(conninfo)) {
        REQUIRE(conn.is_connected());
        conn.disconnect();
        REQUIRE_FALSE(conn.is_connected());
    } else {
        WARN("Cannot connect to Postgres, skipping test");
    }
}

TEST_CASE("Postgres execute basic query", "[PostgresConnector]") {
    PostgresConnector conn;
    std::string conninfo = "host=127.0.0.1 port=15432 dbname=postgres user=postgres password=postgres";
    if (!conn.connect(conninfo)) {
        WARN("Cannot connect to Postgres, skipping test");
        return;
    }

    QueryResult result = conn.execute("SELECT 1 AS test");
    REQUIRE(result.count >= 1);
    REQUIRE(result.columns.size() == 1);
    REQUIRE(result.columns[0].name == "test");
    conn.disconnect();
}

TEST_CASE("Postgres transaction test") {
    PostgresConnector conn;
    std::string conninfo = "host=127.0.0.1 port=15432 dbname=postgres user=postgres password=postgres";

    if (!conn.connect(conninfo)) {
        WARN("Cannot connect to Postgres, skipping test");
        return;
    }

    bool in_transaction = conn.begin_transaction();
    REQUIRE(in_transaction);
    REQUIRE(conn.is_in_transaction());
    // bool batch_execution = conn.execute_batch("SELECT 1 AS test", "INVALID SQL");
    // REQUIRE_FALSE(conn.is_in_transaction());

}
