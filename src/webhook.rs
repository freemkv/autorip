use crate::config::Config;

pub fn send(cfg: &Config, event: &str, title: &str, detail: &str) {
    for hook in &cfg.webhooks {
        if hook.events.contains(&event.to_string()) {
            let body = serde_json::json!({
                "event": event,
                "title": title,
                "detail": detail,
            });
            let _ = ureq::post(&hook.url)
                .set("Content-Type", "application/json")
                .send_string(&body.to_string());
        }
    }
}
