//! Web API tests with the JSON file storage provider: data must survive a restart.

mod common;

use std::path::Path;

use axum::http::StatusCode;
use common::{app, open_config};
use serde_json::json;

fn jsonfile_config(dir: &Path) -> String {
    open_config(&format!(
        "    type: \"JSONFile\"\n    jsonfile:\n        dataDirectory: \"{}\"",
        dir.display().to_string().replace('\\', "/")
    ))
}

#[tokio::test]
async fn data_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let stream;
    {
        let app = app(&jsonfile_config(dir.path())).await;
        let r = app
            .send_json(
                "POST",
                "/api/v1/stream",
                &[],
                json!({"properties": {"name": "persistent"}}),
            )
            .await;
        assert_eq!(r.status, StatusCode::CREATED);
        stream = r.json()["uuid"].as_str().unwrap().to_string();
        let records: Vec<_> = (1..=50)
            .map(|i| json!({"n": i, "text": "line\nbreak"}))
            .collect();
        let r = app
            .send_json(
                "PUT",
                &format!("/api/v1/stream/{stream}/records"),
                &[],
                json!(records),
            )
            .await;
        assert_eq!(r.status, StatusCode::ACCEPTED);
        let r = app
            .send_json(
                "PATCH",
                &format!("/api/v1/stream/{stream}/properties"),
                &[],
                json!({"properties": {"env": "test"}}),
            )
            .await;
        assert_eq!(r.status, StatusCode::OK);
        app.wait_readable(&stream, 50).await;
        // pending records are written when the server stops
        let r = app
            .send_json(
                "PUT",
                &format!("/api/v1/stream/{stream}/record"),
                &[],
                json!({"n": 51}),
            )
            .await;
        assert_eq!(r.json()["messageIds"], json!([51]));
        app.state.service.stop().await;
    }

    let stream_directory = dir.path().join("streams").join(&stream);
    assert!(stream_directory.join("data.jsonl").exists());
    assert!(stream_directory.join("index.bin").exists());
    assert!(stream_directory.join("stream.json").exists());

    let app = app(&jsonfile_config(dir.path())).await;
    assert_eq!(
        app.get("/api/v1/streams", &[]).await.json(),
        json!([stream])
    );
    let info = app
        .get(&format!("/api/v1/stream/{stream}"), &[])
        .await
        .json();
    assert_eq!(
        info["properties"],
        json!({"name": "persistent", "env": "test"})
    );
    assert_eq!(info["readableMessages"]["cptMessages"], 51);
    assert_eq!(info["readableMessages"]["lastMsgId"], 51);

    // message ids continue after the restart
    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{stream}/record"),
            &[],
            json!({"n": 52}),
        )
        .await;
    assert_eq!(r.json()["messageIds"], json!([52]));
    app.wait_readable(&stream, 52).await;

    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{stream}/iterator"),
            &[],
            json!({"iteratorType": "AFTER_MESSAGE_ID", "messageId": 48}),
        )
        .await;
    let iterator = r.json()["streamIteratorUUID"].as_str().unwrap().to_string();
    let r = app
        .get(
            &format!("/api/v1/stream/{stream}/iterator/{iterator}/records"),
            &[],
        )
        .await
        .json();
    let ids: Vec<_> = r["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["i"].as_u64().unwrap())
        .collect();
    assert_eq!(ids, vec![49, 50, 51, 52]);
    assert_eq!(r["records"][0]["m"]["text"], "line\nbreak");

    let r = app
        .call(
            "POST",
            &format!("/api/v1/stream/{stream}/index/rebuild"),
            &[],
            "",
        )
        .await
        .json();
    assert_eq!(r["indexStats"]["CptMessages"], 52);

    let r = app
        .call("DELETE", &format!("/api/v1/stream/{stream}"), &[], "")
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    assert!(!stream_directory.exists());
    app.state.service.stop().await;
}

/// Data written by the Go implementation of ministream (local time offsets in the dates,
/// typo "fastMsgTimestamp", and an index written in the 20 bytes rows format of the Go writer)
/// is loaded, the index is rebuilt automatically.
#[tokio::test]
async fn go_data_compatibility() {
    let dir = tempfile::tempdir().unwrap();
    let uuid = "4ce589e2-b483-467b-8b59-758b339801db";
    let stream_directory = dir.path().join("streams").join(uuid);
    std::fs::create_dir_all(&stream_directory).unwrap();
    std::fs::write(
        dir.path().join("streams.json"),
        format!(r#"{{"streams":["{uuid}"]}}"#),
    )
    .unwrap();
    std::fs::write(
        stream_directory.join("stream.json"),
        format!(
            r#"{{"uuid":"{uuid}","creationDate":"2023-01-15T10:20:30.123456789+01:00","lastUpdate":"2023-01-15T10:20:30.123456789+01:00","properties":{{"name":"from go"}},"ingestedMessages":{{"cptMessages":3,"sizeInBytes":30,"firstMsgId":1,"lastMsgId":3,"firstMsgTimestamp":"2023-01-15T10:21:00+01:00","fastMsgTimestamp":"2023-01-15T10:23:00+01:00"}},"readableMessages":{{"cptMessages":3,"sizeInBytes":30,"firstMsgId":1,"lastMsgId":3,"firstMsgTimestamp":"2023-01-15T10:21:00+01:00","fastMsgTimestamp":"2023-01-15T10:23:00+01:00"}}}}"#
        ),
    )
    .unwrap();
    let lines = [
        r#"{"i":1,"d":"2023-01-15T10:21:00.5+01:00","m":{"v":1}}"#,
        r#"{"i":2,"d":"2023-01-15T10:22:00.5+01:00","m":{"v":2}}"#,
        r#"{"i":3,"d":"2023-01-15T10:23:00.5+01:00","m":{"v":3}}"#,
    ];
    let mut data = String::new();
    let mut index = Vec::new();
    for (n, line) in lines.iter().enumerate() {
        data.push_str(line);
        data.push('\n');
        // row of the Go writer: id (u64), length (i32), unix seconds (i64)
        index.extend_from_slice(&(n as u64 + 1).to_le_bytes());
        index.extend_from_slice(&(line.len() as i32 + 1).to_le_bytes());
        index.extend_from_slice(&1_673_774_460i64.to_le_bytes());
    }
    std::fs::write(stream_directory.join("data.jsonl"), data).unwrap();
    std::fs::write(stream_directory.join("index.bin"), index).unwrap();

    let app = app(&jsonfile_config(dir.path())).await;
    let info = app.get(&format!("/api/v1/stream/{uuid}"), &[]).await.json();
    assert_eq!(info["properties"]["name"], "from go");
    assert_eq!(info["creationDate"], "2023-01-15T09:20:30.123456789Z");

    let r = app
        .send_json(
            "POST",
            &format!("/api/v1/stream/{uuid}/iterator"),
            &[],
            json!({"iteratorType": "AT_TIMESTAMP", "timestamp": "2023-01-15T09:22:00Z"}),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text);
    let iterator = r.json()["streamIteratorUUID"].as_str().unwrap().to_string();
    let r = app
        .get(
            &format!("/api/v1/stream/{uuid}/iterator/{iterator}/records"),
            &[],
        )
        .await
        .json();
    let values: Vec<_> = r["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["m"]["v"].clone())
        .collect();
    assert_eq!(values, vec![json!(2), json!(3)]);

    let r = app
        .send_json(
            "PUT",
            &format!("/api/v1/stream/{uuid}/record"),
            &[],
            json!({"v": 4}),
        )
        .await;
    assert_eq!(r.json()["messageIds"], json!([4]));
    app.state.service.stop().await;
}
