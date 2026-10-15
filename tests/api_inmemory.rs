//! Web API tests with the in memory storage provider and without security.

mod common;

use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::{app, open_config, IN_MEMORY_STORAGE};
use ministream::web::ServerCommand;
use serde_json::{json, Value};

#[tokio::test]
async fn misc_routes() {
    let app = app(&open_config(IN_MEMORY_STORAGE)).await;
    let r = app.get("/", &[]).await;
    assert_eq!(
        (r.status, r.text.as_str()),
        (StatusCode::OK, "Welcome to ministream!")
    );
    assert_eq!(app.get("/api/v1/utils/ping", &[]).await.text, "ok");
    assert_eq!(app.get("/livez", &[]).await.text, "OK");
    assert_eq!(app.get("/readyz", &[]).await.status, StatusCode::OK);

    let r = app.get("/nowhere", &[]).await;
    assert_eq!(
        (r.status, r.text.as_str()),
        (StatusCode::NOT_FOUND, "Cannot GET /nowhere")
    );

    // monitoring routes are disabled in this configuration
    assert_eq!(app.get("/metrics", &[]).await.status, StatusCode::NOT_FOUND);
    assert_eq!(
        app.get("/docs/doc.json", &[]).await.status,
        StatusCode::NOT_FOUND
    );

    // JWT is disabled: login is not possible
    let r = app.get("/api/v1/account/login", &[("API-KEY", "x")]).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["code"], 1202);

    // RBAC is disabled
    let r = app.get("/api/v1/users/", &[]).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["code"], 1213);

    let r = app.get("/api/v1/account", &[]).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["data"]["id"], app.state.account.id.to_string());
}

#[tokio::test]
async fn utils_pbkdf2() {
    let app = app(&open_config(IN_MEMORY_STORAGE)).await;
    let body = json!({"digest": "sha256", "iterations": 1000, "salt": "randomStringAbcdef012346789", "password": "thisIsMySecretPassword"});
    let r = app
        .send_json("POST", "/api/v1/utils/pbkdf2", &[], body)
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.json()["hash"],
        "$pbkdf2-sha256$i=1000$randomStringAbcdef012346789$e7ef1350732294958b0801ad8f389703a93048a90f36260a6212f1e6bb9f68cf"
    );

    let r = app
        .send_json(
            "POST",
            "/api/v1/utils/pbkdf2",
            &[],
            json!({"digest": "sha256", "iterations": 0, "salt": "", "password": "x"}),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["validationErrors"].as_array().unwrap().len(), 2);

    let r = app
        .send_json(
            "POST",
            "/api/v1/utils/pbkdf2",
            &[],
            json!({"digest": "sha3", "iterations": 1, "salt": "s", "password": "x"}),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["code"], 1006);
}

async fn create_stream(app: &common::TestApp, properties: Value) -> String {
    let r = app
        .send_json(
            "POST",
            "/api/v1/stream/",
            &[],
            json!({ "properties": properties }),
        )
        .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.text);
    r.json()["uuid"].as_str().unwrap().to_string()
}

async fn create_iterator(app: &common::TestApp, stream: &str, request: Value) -> String {
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            request,
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    let body = r.json();
    assert_eq!(body["status"], "success");
    body["streamIteratorUUID"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn stream_lifecycle() {
    let app = app(&open_config(IN_MEMORY_STORAGE)).await;
    assert_eq!(app.get("/api/v1/streams/", &[]).await.json(), json!([]));

    let stream = create_stream(&app, json!({"name": "test", "env": "dev"})).await;
    let other = create_stream(&app, json!({"name": "other"})).await;

    // list and filter the streams
    assert_eq!(
        app.get("/api/v1/streams", &[]).await.json(),
        json!([stream, other])
    );
    let r = app
        .get("/api/v1/streams/?jq=.name%20%3D%3D%20%22test%22", &[])
        .await;
    assert_eq!(r.json(), json!([stream]));
    let r = app.get("/api/v1/streams/?jq=.name%20%3D%3D", &[]).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["code"], 1004);
    let r = app
        .get("/api/v1/streams/properties?jq=has(%22env%22)", &[])
        .await;
    let body = r.json();
    assert_eq!(body["result"]["total"], 1);
    assert_eq!(body["result"]["rows"][0]["uuid"], stream);
    assert_eq!(body["result"]["rows"][0]["properties"]["env"], "dev");

    // properties
    let uri = format!("/api/v1/stream/{stream}/properties");
    assert_eq!(
        app.get(&uri, &[]).await.json(),
        json!({"name": "test", "env": "dev"})
    );
    let r = app
        .send_json(
            "PATCH",
            &uri,
            &[],
            json!({"properties": {"env": "prod", "team": "a"}}),
        )
        .await;
    assert_eq!(
        r.json(),
        json!({"name": "test", "env": "prod", "team": "a"})
    );
    let r = app
        .send_json(
            "POST",
            &uri,
            &[],
            json!({"properties": {"name": "renamed"}}),
        )
        .await;
    assert_eq!(r.json(), json!({"name": "renamed"}));
    let r = app
        .send_json("POST", &uri, &[], json!({"properties": {"name": 3}}))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["code"], 1007);
    let r = app
        .send_json("POST", "/api/v1/stream", &[], json!({"nothing": true}))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["validationErrors"][0]["failedField"], "Properties");

    // invalid or unknown streams
    let r = app.get("/api/v1/stream/not-a-uuid", &[]).await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1000))
    );
    let r = app
        .get("/api/v1/stream/9cf1eba6-0f6d-425e-ada9-e5b6cf293ffc", &[])
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1001))
    );
    assert_eq!(
        r.json()["streamUUID"],
        "9cf1eba6-0f6d-425e-ada9-e5b6cf293ffc"
    );

    // information
    let info = app
        .get(&format!("/api/v1/stream/{stream}"), &[])
        .await
        .json();
    assert_eq!(info["uuid"], stream);
    assert_eq!(info["ingestedMessages"]["cptMessages"], 0);

    // delete
    let r = app
        .call("DELETE", &format!("/api/v1/stream/{other}"), &[], "")
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json(), json!({"code": 200, "message": "success"}));
    assert_eq!(
        app.get("/api/v1/streams/", &[]).await.json(),
        json!([stream])
    );
    let r = app
        .call("DELETE", &format!("/api/v1/stream/{other}"), &[], "")
        .await;
    assert_eq!(r.json()["code"], 1001);
}

#[tokio::test]
async fn put_and_get_records() {
    let app = app(&open_config(IN_MEMORY_STORAGE)).await;
    let stream = create_stream(&app, json!({"name": "records"})).await;

    // put records: single record, JSON array, JSON lines
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/record"),
            &[],
            json!({"a": 1}),
        )
        .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{}", r.text);
    assert_eq!(r.json()["messageIds"], json!([1]));
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &[],
            json!([{"a": 2}, {"a": 3}]),
        )
        .await;
    assert_eq!(r.json()["messageIds"], json!([2, 3]));
    assert_eq!(r.json()["count"], 2);
    let r = app
        .call(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &[("content-type", "application/x-ndjson")],
            "{\"a\": 4}\n{\"a\": 5}\n",
        )
        .await;
    assert_eq!(r.json()["messageIds"], json!([4, 5]));

    // invalid bodies
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/record"),
            &[],
            json!([1]),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1007))
    );
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &[],
            json!({"a": 1}),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1012))
    );
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &[],
            json!([]),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1011))
    );

    // batch deduplication
    let batch = [("x-ministream-batch-id", "batch-1")];
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &batch,
            json!([{"a": 6}]),
        )
        .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &batch,
            json!([{"a": 6}]),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1008))
    );
    // a failed batch can be sent again
    let batch = [("x-ministream-batch-id", "batch-2")];
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &batch,
            json!("bad"),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &batch,
            json!([{"a": 7}]),
        )
        .await;
    assert_eq!(r.status, StatusCode::ACCEPTED);
    app.wait_readable(&stream, 7).await;

    // read the records by pages
    let iterator = create_iterator(
        &app,
        &stream,
        json!({"iteratorType": "FIRST_MESSAGE", "name": "reader"}),
    )
    .await;
    let records_uri = format!("/api/v1/stream/{stream}/iterator/{iterator}/records");
    let r = app
        .get(&format!("{records_uri}?maxRecords=2"), &[])
        .await
        .json();
    assert_eq!(
        (
            r["count"].clone(),
            r["remain"].clone(),
            r["lastRecordIdRead"].clone()
        ),
        (json!(2), json!(true), json!(2))
    );
    assert_eq!(r["records"][0]["i"], 1);
    assert_eq!(r["records"][0]["m"], json!({"a": 1}));
    assert!(r["records"][0]["d"].is_string());
    let r = app.get(&records_uri, &[]).await.json();
    assert_eq!(
        (
            r["count"].clone(),
            r["remain"].clone(),
            r["lastRecordIdRead"].clone()
        ),
        (json!(5), json!(false), json!(7))
    );
    let r = app.get(&records_uri, &[]).await.json();
    assert_eq!(
        (r["count"].clone(), r["lastRecordIdRead"].clone()),
        (json!(0), json!(7))
    );

    // invalid maxRecords values
    for value in ["0", "101", "abc", "-1"] {
        let r = app
            .get(&format!("{records_uri}?maxRecords={value}"), &[])
            .await;
        assert_eq!(
            (r.status, r.json()["code"].clone()),
            (StatusCode::BAD_REQUEST, json!(1006)),
            "{value}"
        );
    }

    // statistics
    let r = app
        .get(
            &format!("/api/v1/stream/{stream}/iterator/{iterator}/stats"),
            &[],
        )
        .await
        .json();
    assert_eq!(r["lastRecordIdRead"], 7);
    assert_eq!(r["name"], "reader");
    assert_eq!(r["stats"]["recordsSent"], 7);

    // jq filter on the records
    let filtered = create_iterator(
        &app,
        &stream,
        json!({"iteratorType": "FIRST_MESSAGE", "jqFilter": "select(.m.a > 4) | .m.a"}),
    )
    .await;
    let r = app
        .get(
            &format!("/api/v1/stream/{stream}/iterator/{filtered}/records"),
            &[],
        )
        .await
        .json();
    assert_eq!(r["records"], json!([5, 6, 7]));
    assert_eq!(r["countSkipped"], 4);

    // other iterator types
    let at = create_iterator(
        &app,
        &stream,
        json!({"iteratorType": "AT_MESSAGE_ID", "messageId": 6}),
    )
    .await;
    let r = app
        .get(
            &format!("/api/v1/stream/{stream}/iterator/{at}/records"),
            &[],
        )
        .await
        .json();
    assert_eq!(r["records"].as_array().unwrap().len(), 2);
    let last = create_iterator(&app, &stream, json!({"iteratorType": "LAST_MESSAGE"})).await;
    let r = app
        .get(
            &format!("/api/v1/stream/{stream}/iterator/{last}/records"),
            &[],
        )
        .await
        .json();
    assert_eq!(r["records"][0]["i"], 7);
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            json!({"iteratorType": "AT_MESSAGE_ID", "messageId": 99}),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1014))
    );
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            json!({"iteratorType": "SOMEWHERE"}),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1013))
    );
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            json!({"iteratorType": "FIRST_MESSAGE", "jqFilter": ".a =="}),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1013))
    );

    // close an iterator
    let r = app
        .call(
            "DELETE",
            &format!("/api/v1/stream/{stream}/iterator/{iterator}"),
            &[],
            "",
        )
        .await;
    assert_eq!(r.json()["message"], "Stream iterator closed");
    let r = app
        .call(
            "DELETE",
            &format!("/api/v1/stream/{stream}/iterator/{iterator}"),
            &[],
            "",
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1030))
    );
    let r = app.get(&records_uri, &[]).await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1031))
    );

    // stream information
    let info = app
        .get(&format!("/api/v1/stream/{stream}"), &[])
        .await
        .json();
    assert_eq!(info["ingestedMessages"]["cptMessages"], 7);
    assert_eq!(info["readableMessages"]["cptMessages"], 7);
    assert_eq!(info["readableMessages"]["lastMsgId"], 7);

    // index rebuild (no index for the in memory storage)
    let r = app
        .call(
            "POST",
            &format!("/api/v1/stream/{stream}/index/rebuild"),
            &[],
            "",
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["indexStats"], "");
}

#[tokio::test]
async fn iterators_limit() {
    let app = app(&open_config(IN_MEMORY_STORAGE)).await;
    let stream = create_stream(&app, json!({})).await;
    for _ in 0..10 {
        create_iterator(&app, &stream, json!({"iteratorType": "FIRST_MESSAGE"})).await;
    }
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            json!({"iteratorType": "FIRST_MESSAGE"}),
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1014))
    );
}

#[tokio::test]
async fn long_polling() {
    let app = app(&open_config(IN_MEMORY_STORAGE)).await;
    let stream = create_stream(&app, json!({})).await;
    let iterator = create_iterator(
        &app,
        &stream,
        json!({"iteratorType": "AFTER_LAST_MESSAGE", "maxWaitTimeSeconds": 10}),
    )
    .await;
    let records_uri = format!("/api/v1/stream/{stream}/iterator/{iterator}/records");

    let start = Instant::now();
    let (response, busy) = tokio::join!(app.get(&records_uri, &[]), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        // the iterator is busy while a request is waiting for records
        let busy = app.get(&records_uri, &[]).await;
        app.send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/record"),
            &[],
            json!({"late": true}),
        )
        .await;
        busy
    });
    assert_eq!(busy.status.as_u16(), 425);
    assert_eq!(busy.json()["code"], 1032);
    let r = response.json();
    assert_eq!(r["count"], 1, "{r}");
    assert_eq!(r["records"][0]["m"]["late"], true);
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the request must return as soon as a record is available"
    );

    // closing the iterator interrupts a waiting request
    let start = Instant::now();
    let (response, _) = tokio::join!(app.get(&records_uri, &[]), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        app.call(
            "DELETE",
            &format!("/api/v1/stream/{stream}/iterator/{iterator}"),
            &[],
            "",
        )
        .await
    });
    assert_eq!(response.json()["count"], 0);
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn max_allowed_streams() {
    let config = open_config(IN_MEMORY_STORAGE).replace(
        "maxMessagePerGetOperation: 100",
        "maxMessagePerGetOperation: 100\n    maxAllowedStreams: 1",
    );
    let app = app(&config).await;
    create_stream(&app, json!({})).await;
    let r = app
        .send_json("POST", "/api/v1/stream/", &[], json!({"properties": {}}))
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1002))
    );
}

#[tokio::test]
async fn admin_commands() {
    let mut app = app(&open_config(IN_MEMORY_STORAGE)).await;
    let r = app
        .call("POST", "/api/v1/admin/server/restart", &[], "")
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(app.commands.recv().await, Some(ServerCommand::Restart));
    let r = app
        .call("POST", "/api/v1/admin/server/shutdown", &[], "")
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(app.commands.recv().await, Some(ServerCommand::Shutdown));
    let r = app.call("POST", "/api/v1/admin/jwt/revoke", &[], "").await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn optional_routes_and_middlewares() {
    let config = open_config(IN_MEMORY_STORAGE).replace(
        "webserver:\n",
        "webserver:\n    metrics:\n        enable: true\n    monitor:\n        enable: true\n    swagger:\n        enable: true\n    cors:\n        enable: true\n        allowOrigins: \"*\"\n    rateLimiter:\n        enable: true\n        routeStream:\n            maxRequests: 2\n            durationInSeconds: 60\n",
    );
    let app = app(&config).await;

    let r = app.get("/docs/doc.json", &[]).await;
    assert_eq!(r.json()["swagger"], "2.0");
    assert!(app
        .get("/docs/index.html", &[])
        .await
        .text
        .contains("swagger-ui"));
    let r = app.get("/monitor", &[("accept", "application/json")]).await;
    assert!(r.json()["ministream"]["uptime_seconds"].is_u64());
    assert!(app.get("/monitor", &[]).await.text.contains("<html"));

    let r = app
        .get(
            "/api/v1/stream/not-a-uuid",
            &[("origin", "https://example.com")],
        )
        .await;
    assert_eq!(r.headers["access-control-allow-origin"], "*");

    // the rate limiter allows 2 requests per minute and per route (failed requests are not counted)
    assert_eq!(
        app.get("/api/v1/streams/?jq=.a%3D%3D", &[]).await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.get("/api/v1/streams/", &[]).await.status,
        StatusCode::OK
    );
    assert_eq!(
        app.get("/api/v1/streams/", &[]).await.status,
        StatusCode::OK
    );
    let r = app.get("/api/v1/streams/", &[]).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(r.headers.contains_key("retry-after"));

    let metrics = app.get("/metrics", &[]).await.text;
    assert!(metrics.contains("ministream_streams 0"), "{metrics}");
    assert!(
        metrics.contains("ministream_http_requests_total{method=\"GET\",code=\"429\"}"),
        "{metrics}"
    );
    assert!(metrics.contains("process_resident_memory_bytes"));
}
