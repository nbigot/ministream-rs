//! Swagger documentation of the API (`/docs/index.html` and `/docs/doc.json`).

use axum::http::header;
use axum::response::{Html, IntoResponse, Redirect, Response};

/// OpenAPI (swagger 2.0) specification of the API.
pub const SWAGGER_JSON: &str = include_str!("../../docs/swagger.json");

const SWAGGER_UI_VERSION: &str = "5.17.14";

pub async fn index() -> Html<String> {
    Html(format!(
        r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MiniStream API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@{SWAGGER_UI_VERSION}/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@{SWAGGER_UI_VERSION}/swagger-ui-bundle.js"></script>
<script>
window.ui = SwaggerUIBundle({{ url: "/docs/doc.json", dom_id: "#swagger-ui", deepLinking: false }});
</script>
</body>
</html>
"##
    ))
}

pub async fn doc_json() -> Response {
    ([(header::CONTENT_TYPE, "application/json")], SWAGGER_JSON).into_response()
}

pub async fn redirect_to_index() -> Redirect {
    Redirect::temporary("/docs/index.html")
}

#[cfg(test)]
mod tests {
    #[test]
    fn swagger_spec_is_valid_json() {
        let spec: serde_json::Value = serde_json::from_str(super::SWAGGER_JSON).unwrap();
        assert!(spec["paths"]["/api/v1/stream/{streamuuid}/index/rebuild"].is_object());
    }
}
