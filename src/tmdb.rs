pub struct TmdbResult {
    pub title: String,
    pub year: u16,
    pub poster_url: String,
    pub overview: String,
}

pub fn lookup(_title: &str, _api_key: &str) -> Option<TmdbResult> {
    None
}
