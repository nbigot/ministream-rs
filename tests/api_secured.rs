//! Web API tests with JWT, authentication and RBAC/ABAC enabled.

mod common;

use std::path::Path;
use std::time::Duration;

use axum::http::StatusCode;
use common::{app, TestApp};
use ministream::auth::hash_password;
use serde_json::{json, Value};

const API_KEY: &str = "test-secret-api-key";

/// Secured server using the RBAC rules of the "secured-inmemory" template
/// and generated credentials (the password of each user is "<user>-password").
async fn secured_app(dir: &Path) -> TestApp {
    let mut secrets = serde_json::Map::new();
    secrets.insert("_WARNING".into(), json!("test credentials"));
    for user in ["admin", "dba", "producer", "consumer", "demo"] {
        let hash = hash_password(
            "sha256",
            10,
            &format!("salt-{user}"),
            &format!("{user}-password"),
        )
        .unwrap();
        secrets.insert(user.into(), json!(hash));
    }
    let secrets_path = dir.join("secrets.json");
    std::fs::write(&secrets_path, Value::Object(secrets).to_string()).unwrap();
    let rbac_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("config-templates/secured-inmemory/data/secrets/rbac.json");

    app(&format!(
        r#"
logger:
    level: "off"
auth:
    enable: true
    method: "FILE"
    methods:
        file:
            filename: "{}"
account:
    name: "secured"
    secretAPIKey: "{API_KEY}"
rbac:
    enable: true
    filename: "{}"
auditlog:
    enable: true
    enableLogAccessGranted: true
storage:
    type: "InMemory"
    logger:
        level: "off"
streams:
    bulkFlushFrequency: 0
    bulkMaxSize: 100
    channelBufferSize: 100
    maxMessagePerGetOperation: 100
webserver:
    jwt:
        enable: true
        secretKey: "a test secret"
        tokenExpireInMinutes: 5
        iss: "ministream"
        sub: "auth"
        aud: "ministream"
"#,
        secrets_path.display().to_string().replace('\\', "/"),
        rbac_path.display().to_string().replace('\\', "/"),
    ))
    .await
}

async fn login_user(app: &TestApp, user: &str, password: &str) -> common::TestResponse {
    app.get(
        "/api/v1/user/login",
        &[("ACCESS-KEY-ID", user), ("SECRET-ACCESS-KEY", password)],
    )
    .await
}

async fn user_token(app: &TestApp, user: &str) -> String {
    let r = login_user(app, user, &format!("{user}-password")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert_eq!(r.json()["message"], "login user succeeded");
    format!("Bearer {}", r.json()["jwt"].as_str().unwrap())
}

async fn superuser_token(app: &TestApp) -> String {
    let r = app
        .get("/api/v1/account/login", &[("API-KEY", API_KEY)])
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    format!("Bearer {}", r.json()["jwt"].as_str().unwrap())
}

async fn create_stream(app: &TestApp, token: &str, properties: Value) -> String {
    let r = app
        .send_json(
            "POST",
            "/api/v1/stream/",
            &[("authorization", token)],
            json!({ "properties": properties }),
        )
        .await;
    assert_eq!(r.status, StatusCode::CREATED, "{}", r.text);
    r.json()["uuid"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn jwt_is_required() {
    let dir = tempfile::tempdir().unwrap();
    let app = secured_app(dir.path()).await;

    let r = app.get("/api/v1/streams/", &[]).await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::BAD_REQUEST, json!(1200))
    );
    let r = app
        .get(
            "/api/v1/streams/",
            &[("authorization", "Bearer abc.def.ghi")],
        )
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::UNAUTHORIZED, json!(1201))
    );
    let r = app.get("/api/v1/account", &[]).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = app
        .call("POST", "/api/v1/admin/server/shutdown", &[], "")
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    // routes which are not protected
    assert_eq!(
        app.get("/api/v1/utils/ping", &[]).await.status,
        StatusCode::OK
    );
    assert_eq!(app.get("/", &[]).await.status, StatusCode::OK);
}

#[tokio::test]
async fn account_login() {
    let dir = tempfile::tempdir().unwrap();
    let app = secured_app(dir.path()).await;

    let r = app
        .get("/api/v1/account/validate", &[("API-KEY", API_KEY)])
        .await;
    assert_eq!(r.json(), json!({"code": 200, "message": "success"}));
    let r = app
        .get("/api/v1/account/validate", &[("API-KEY", "wrong")])
        .await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::FORBIDDEN, json!(1006))
    );
    let r = app.get("/api/v1/account/login", &[]).await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (StatusCode::FORBIDDEN, json!("wrong credentials"))
    );

    // the super user can do anything
    let token = superuser_token(&app).await;
    let auth = [("authorization", token.as_str())];
    let r = app.get("/api/v1/account/", &auth).await;
    assert_eq!(r.json()["data"]["name"], "secured");
    let r = app.get("/api/v1/users/", &auth).await;
    assert_eq!(
        r.json()["users"],
        json!(["admin", "consumer", "dba", "demo", "producer"])
    );
    let stream = create_stream(&app, &token, json!({"name": "s"})).await;
    let r = app
        .call("DELETE", &format!("/api/v1/stream/{stream}"), &auth, "")
        .await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn user_login() {
    let dir = tempfile::tempdir().unwrap();
    let app = secured_app(dir.path()).await;

    let r = login_user(&app, "producer", "wrong").await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::FORBIDDEN, json!(1230))
    );
    let r = login_user(&app, "nobody", "nobody-password").await;
    assert_eq!(
        (r.status, r.json()["code"].clone()),
        (StatusCode::FORBIDDEN, json!(1230))
    );
    let r = app.get("/api/v1/user/login", &[]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    user_token(&app, "producer").await;
}

#[tokio::test]
async fn rbac_rules() {
    let dir = tempfile::tempdir().unwrap();
    let app = secured_app(dir.path()).await;
    let superuser = superuser_token(&app).await;
    let stream = create_stream(&app, &superuser, json!({"name": "s"})).await;

    // producer: create streams and put records, but cannot read records nor delete streams
    let producer = user_token(&app, "producer").await;
    let auth = [("authorization", producer.as_str())];
    create_stream(&app, &producer, json!({"name": "p"})).await;
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &auth,
            json!([{"x": 1}]),
        )
        .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{}", r.text);
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &auth,
            json!({"iteratorType": "FIRST_MESSAGE"}),
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(
        r.json(),
        json!({
            "error": "rbac action forbidden",
            "details": "CreateRecordsIterator",
            "code": 1212,
            "streamUUID": "00000000-0000-0000-0000-000000000000"
        })
    );
    assert_eq!(
        app.call("DELETE", &format!("/api/v1/stream/{stream}"), &auth, "")
            .await
            .status,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.get("/api/v1/users", &auth).await.status,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.call("POST", "/api/v1/admin/server/shutdown", &auth, "")
            .await
            .status,
        StatusCode::FORBIDDEN
    );

    // consumer: read records only
    let consumer = user_token(&app, "consumer").await;
    let auth = [("authorization", consumer.as_str())];
    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &auth,
            json!({"iteratorType": "FIRST_MESSAGE"}),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let iterator = r.json()["streamIteratorUUID"].as_str().unwrap().to_string();
    app.wait_readable(&stream, 1).await;
    let r = app
        .get(
            &format!("/api/v1/stream/{stream}/iterator/{iterator}/records"),
            &auth,
        )
        .await;
    assert_eq!(r.json()["count"], 1);
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/records"),
            &auth,
            json!([{"x": 1}]),
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(
        app.get("/api/v1/streams/", &auth)
            .await
            .json()
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // dba: delete streams, rebuild indexes, get the account
    let dba = user_token(&app, "dba").await;
    let auth = [("authorization", dba.as_str())];
    assert_eq!(
        app.get("/api/v1/account", &auth).await.status,
        StatusCode::OK
    );
    assert_eq!(
        app.call(
            "POST",
            &format!("/api/v1/stream/{stream}/index/rebuild"),
            &auth,
            ""
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        app.call("DELETE", &format!("/api/v1/stream/{stream}"), &auth, "")
            .await
            .status,
        StatusCode::OK
    );
    assert_eq!(
        app.call("POST", "/api/v1/admin/jwt/revoke", &auth, "")
            .await
            .status,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn abac_rules() {
    let dir = tempfile::tempdir().unwrap();
    let app = secured_app(dir.path()).await;
    let superuser = superuser_token(&app).await;
    let demo_stream =
        create_stream(&app, &superuser, json!({"project": "demo", "env": "test"})).await;
    let other_stream =
        create_stream(&app, &superuser, json!({"project": "other", "env": "test"})).await;

    let demo = user_token(&app, "demo").await;
    let auth = [("authorization", demo.as_str())];

    // the streams are filtered by the ABAC rule of the listing actions
    assert_eq!(
        app.get("/api/v1/streams/", &auth).await.json(),
        json!([demo_stream])
    );
    let r = app.get("/api/v1/streams/properties", &auth).await.json();
    assert_eq!(r["result"]["total"], 1);

    // the ABAC rule is evaluated on the stream of the route
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{demo_stream}/record"),
            &auth,
            json!({"x": 1}),
        )
        .await;
    assert_eq!(r.status, StatusCode::ACCEPTED, "{}", r.text);
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{other_stream}/record"),
            &auth,
            json!({"x": 1}),
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(
        app.get(&format!("/api/v1/stream/{demo_stream}"), &auth)
            .await
            .status,
        StatusCode::OK
    );
    assert_eq!(
        app.get(&format!("/api/v1/stream/{other_stream}"), &auth)
            .await
            .status,
        StatusCode::FORBIDDEN
    );
    // unknown streams are forbidden too
    let r = app
        .get("/api/v1/stream/9cf1eba6-0f6d-425e-ada9-e5b6cf293ffc", &auth)
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);

    // the demo user cannot change the properties to escape the rule
    let r = app
        .send_json(
            "PATCH",
            &format!("/api/v1/stream/{demo_stream}/properties"),
            &auth,
            json!({"properties": {"project": "x"}}),
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn revoke_all_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let app = secured_app(dir.path()).await;
    let admin = user_token(&app, "admin").await;
    let consumer = user_token(&app, "consumer").await;
    assert_eq!(
        app.get("/api/v1/streams/", &[("authorization", consumer.as_str())])
            .await
            .status,
        StatusCode::OK
    );

    // tokens are revoked with a precision of one second
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let r = app
        .call(
            "POST",
            "/api/v1/admin/jwt/revoke",
            &[("authorization", admin.as_str())],
            "",
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let r = app
        .get("/api/v1/streams/", &[("authorization", consumer.as_str())])
        .await;
    assert_eq!(
        (r.status, r.json()["error"].clone()),
        (StatusCode::UNAUTHORIZED, json!("jwt was revoked"))
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    let consumer = user_token(&app, "consumer").await;
    assert_eq!(
        app.get("/api/v1/streams/", &[("authorization", consumer.as_str())])
            .await
            .status,
        StatusCode::OK
    );
}
