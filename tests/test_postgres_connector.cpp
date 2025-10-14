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
