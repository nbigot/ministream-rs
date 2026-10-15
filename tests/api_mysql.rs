//! Web API tests with the MySQL storage provider.
//!
//! These tests need a MySQL server, they are skipped unless the environment variable
//! `MINISTREAM_TEST_MYSQL_DSN` is set, for example:
//!
//! ```sh
//! docker run --name mysql-ministream -e MYSQL_ROOT_PASSWORD=my-secret-pw -e MYSQL_DATABASE=ministream -p 3306:3306 -d mysql:8
//! MINISTREAM_TEST_MYSQL_DSN="root:my-secret-pw@tcp(127.0.0.1:3306)/ministream" cargo test --test api_mysql
//! ```

#![cfg(feature = "mysql")]

mod common;

use axum::http::StatusCode;
use common::{app, open_config};
use ministream::storage::mysql::parse_dsn;
use serde_json::json;

fn mysql_config(dsn: &str, schema: &str) -> String {
    open_config(&format!(
        "    type: \"MySQL\"\n    mysql:\n        dsn: \"{dsn}\"\n        maxOpenConns: 5\n        maxIdleConns: 2\n        schemaName: \"{schema}\"\n        catalogTableName: \"streams\""
    ))
}

#[tokio::test]
async fn mysql_storage() {
    let Ok(dsn) = std::env::var("MINISTREAM_TEST_MYSQL_DSN") else {
        eprintln!("MINISTREAM_TEST_MYSQL_DSN is not set: test skipped");
        return;
    };
    let schema = format!(
        "ministream_test_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let config = mysql_config(&dsn, &schema);

    let stream;
    {
        let app = app(&config).await;
        assert!(app
            .get("/api/v1/streams/", &[])
            .await
            .json()
            .as_array()
            .unwrap()
            .is_empty());
        let r = app
            .send_json(
                "POST",
                "/api/v1/stream/",
                &[],
                json!({"properties": {"name": "mysql"}}),
            )
            .await;
        assert_eq!(r.status, StatusCode::CREATED, "{}", r.text);
        stream = r.json()["uuid"].as_str().unwrap().to_string();

        let records: Vec<_> = (1..=250)
            .map(|i| json!({"n": i, "text": format!("record {i}")}))
            .collect();
        let r = app
            .send_json(
                "PUT",
                &format!("/api/v1/stream/{stream}/records"),
                &[],
                json!(records),
            )
            .await;
        assert_eq!(r.status, StatusCode::ACCEPTED, "{}", r.text);
        app.wait_readable(&stream, 250).await;

        let r = app
            .send_json(
                "PATCH",
                &format!("/api/v1/stream/{stream}/properties"),
                &[],
                json!({"properties": {"env": "test"}}),
            )
            .await;
        assert_eq!(r.status, StatusCode::OK);

        // read every record by pages
        let r = app
            .send_json(
                "POST",
                &format!("/api/v1/stream/{stream}/iterator"),
                &[],
                json!({"iteratorType": "FIRST_MESSAGE"}),
            )
            .await;
        let iterator = r.json()["streamIteratorUUID"].as_str().unwrap().to_string();
        let mut ids = Vec::new();
        loop {
            let r = app
                .get(
                    &format!("/api/v1/stream/{stream}/iterator/{iterator}/records?maxRecords=100"),
                    &[],
                )
                .await
                .json();
            let page: Vec<u64> = r["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["i"].as_u64().unwrap())
                .collect();
            if page.is_empty() {
                break;
            }
            ids.extend(page);
        }
        assert_eq!(ids, (1..=250).collect::<Vec<u64>>());
        app.state.service.stop().await;
    }

    // restart: the catalog, the properties and the records are persisted
    let app = app(&config).await;
    assert_eq!(
        app.get("/api/v1/streams/", &[]).await.json(),
        json!([stream])
    );
    let info = app
        .get(&format!("/api/v1/stream/{stream}"), &[])
        .await
        .json();
    assert_eq!(info["properties"], json!({"name": "mysql", "env": "test"}));
    assert_eq!(info["readableMessages"]["cptMessages"], 250);
    assert_eq!(info["readableMessages"]["lastMsgId"], 250);

    for (request, first, count) in [
        (
            json!({"iteratorType": "AT_MESSAGE_ID", "messageId": 200}),
            200,
            51,
        ),
        (
            json!({"iteratorType": "AFTER_MESSAGE_ID", "messageId": 200}),
            201,
            50,
        ),
        (json!({"iteratorType": "LAST_MESSAGE"}), 250, 1),
        (
            json!({"iteratorType": "AT_TIMESTAMP", "timestamp": "2000-01-01T00:00:00Z"}),
            1,
            100,
        ),
    ] {
        let r = app
            .send_json(
                "POST",
                &format!("/api/v1/stream/{stream}/iterator"),
                &[],
                request.clone(),
            )
            .await;
        assert_eq!(r.status, StatusCode::OK, "{request}: {}", r.text);
        let iterator = r.json()["streamIteratorUUID"].as_str().unwrap().to_string();
        let r = app
            .get(
                &format!("/api/v1/stream/{stream}/iterator/{iterator}/records"),
                &[],
            )
            .await
            .json();
        assert_eq!(r["count"], count, "{request}");
        assert_eq!(r["records"][0]["i"], first, "{request}");
        assert_eq!(r["records"][0]["m"]["text"], format!("record {first}"));
    }
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            json!({"iteratorType": "AT_MESSAGE_ID", "messageId": 999}),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    // message ids continue after the restart
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/record"),
            &[],
            json!({"n": 251}),
        )
        .await;
    assert_eq!(r.json()["messageIds"], json!([251]));
    app.wait_readable(&stream, 251).await;

    let r = app
        .call(
            "POST",
            &format!("/api/v1/stream/{stream}/index/rebuild"),
            &[],
            "",
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.json()["indexStats"]["CptMessages"], 251);
    assert_eq!(r.json()["indexStats"]["LastMsgId"], 251);

    let r = app
        .call("DELETE", &format!("/api/v1/stream/{stream}"), &[], "")
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(app.get("/api/v1/streams/", &[]).await.json(), json!([]));
    app.state.service.stop().await;

    // cleanup
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .connect_with(parse_dsn(&dsn).unwrap())
        .await
        .unwrap();
    sqlx::query(&format!("DROP SCHEMA `{schema}`"))
        .execute(&pool)
        .await
        .unwrap();
}
