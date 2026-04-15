#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TmdbResult {
    pub title: String,
    pub year: u16,
    pub poster_url: String,
    pub overview: String,
    pub media_type: String, // "movie" or "tv"
}

pub fn lookup(query: &str, api_key: &str) -> Option<TmdbResult> {
    if api_key.is_empty() {
        return None;
    }

    let url = format!(
        "https://api.themoviedb.org/3/search/multi?api_key={}&query={}&page=1",
        api_key,
        urlencoded(query)
    );

    let resp: serde_json::Value = ureq::get(&url).call().ok()?.into_json().ok()?;
    let results = resp["results"].as_array()?;
    let first = results.first()?;

    let media_type = first["media_type"].as_str().unwrap_or("movie");
    let title = first
        .get(if media_type == "tv" { "name" } else { "title" })?
        .as_str()?
        .to_string();
    let date = first
        .get(if media_type == "tv" {
            "first_air_date"
        } else {
            "release_date"
        })?
        .as_str()
        .unwrap_or("");
    let year: u16 = date.get(..4).and_then(|y| y.parse().ok()).unwrap_or(0);
    let poster = first["poster_path"]
        .as_str()
        .map(|p| format!("https://image.tmdb.org/t/p/w300{}", p))
        .unwrap_or_default();
    let overview = first["overview"].as_str().unwrap_or("").to_string();

    Some(TmdbResult {
        title,
        year,
        poster_url: poster,
        overview,
        media_type: media_type.to_string(),
    })
}

/// Clean a disc label for TMDB search: "DUNE_PART_TWO" -> "Dune Part Two"
pub fn clean_title(label: &str) -> String {
    label
        .replace(['_', '-'], " ")
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn urlencoded(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => (b as char).to_string(),
            b' ' => "+".to_string(),
            _ => format!("%{:02X}", b),
        })
        .collect()
}
