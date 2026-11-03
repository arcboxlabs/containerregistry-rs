//! Registry client core: configuration, transport, authentication and retries.
//!
//! Operations live in sibling modules (`manifests`, `blobs`, `upload`,
//! `listing`) as additional `impl Client` blocks; this module owns the request
//! pipeline every operation goes through:
//!
//! 1. resolve credentials for the registry/repository through a [`Keychain`],
//! 2. attach an `Authorization` header (Basic, a cached Bearer token scoped to
//!    the operation, or nothing),
//! 3. on `401`, learn the `WWW-Authenticate` challenge, fetch a token for the
//!    required scopes and replay the request,
//! 4. retry transport errors, `408`, `429` and `5xx` responses with
//!    exponential backoff (honouring `Retry-After`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, RETRY_AFTER, WWW_AUTHENTICATE};
use reqwest::{Client as HttpClient, RequestBuilder, Response, StatusCode, Url};
use serde::Deserialize;
use tracing::{debug, trace, warn};

use containerregistry_auth::{Credential, Keychain};
use containerregistry_image::{Algorithm, Digest};

use crate::metrics::{MetricsCollector, Operation};
use crate::reference::Reference;
use crate::{Error, Result};

/// Default connect and idle-read timeout for HTTP requests.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default number of retry attempts.
const DEFAULT_RETRIES: u32 = 3;

/// User-Agent header value.
const USER_AGENT_VALUE: &str = concat!("containerregistry-rs/", env!("CARGO_PKG_VERSION"));

/// OCI distribution API version prefix.
const API_PREFIX: &str = "/v2";

/// Configuration for the registry client.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Connect timeout and idle timeout between reads of a response body.
    ///
    /// This is deliberately *not* a total request timeout: a multi-gigabyte
    /// blob transfer may take far longer than any fixed bound, but should
    /// fail fast when the connection stalls.
    pub timeout: Duration,

    /// Number of retry attempts for transient errors.
    pub retries: u32,

    /// Whether to use HTTPS (default: true). Even when true, loopback and
    /// private-network registries (`localhost`, `127.0.0.1`, RFC 1918
    /// addresses, `*.local`) use plain HTTP, matching go-containerregistry.
    pub https: bool,

    /// Whether to accept invalid TLS certificates.
    pub insecure: bool,

    /// User-Agent sent with every request (prefixing the library's own).
    pub user_agent: Option<String>,

    /// Base delay for exponential backoff between retries.
    pub retry_backoff: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            retries: DEFAULT_RETRIES,
            https: true,
            insecure: false,
            user_agent: None,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

impl ClientConfig {
    /// Creates a new client configuration with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the connect/idle timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the number of retry attempts.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Sets whether to use HTTPS.
    pub fn with_https(mut self, https: bool) -> Self {
        self.https = https;
        self
    }

    /// Sets whether to accept invalid TLS certificates.
    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

    /// Sets a User-Agent prefix.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Sets the base retry backoff.
    pub fn with_retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }
}

/// The scope string for pulling from `repository`.
pub(crate) fn pull_scope(repository: &str) -> String {
    format!("repository:{}:pull", repository)
}

/// The scope string for pushing to `repository`.
pub(crate) fn push_scope(repository: &str) -> String {
    format!("repository:{}:pull,push", repository)
}

/// The scope string for deleting from `repository`.
pub(crate) fn delete_scope(repository: &str) -> String {
    format!("repository:{}:delete", repository)
}

/// The scope string for listing the registry catalog.
pub(crate) const CATALOG_SCOPE: &str = "registry:catalog:*";

/// What a request is addressed to, for authentication purposes.
#[derive(Clone, Debug)]
pub(crate) struct Target {
    pub registry: String,
    pub repository: Option<String>,
    pub scopes: Vec<String>,
}

impl Target {
    pub(crate) fn registry(registry: &str) -> Self {
        Self {
            registry: registry.to_string(),
            repository: None,
            scopes: Vec::new(),
        }
    }

    pub(crate) fn pull(reference: &Reference) -> Self {
        Self {
            registry: reference.registry().to_string(),
            repository: Some(reference.repository().to_string()),
            scopes: vec![pull_scope(reference.repository())],
        }
    }

    pub(crate) fn push(reference: &Reference) -> Self {
        Self {
            registry: reference.registry().to_string(),
            repository: Some(reference.repository().to_string()),
            scopes: vec![push_scope(reference.repository())],
        }
    }

    pub(crate) fn with_scope(mut self, scope: String) -> Self {
        if !self.scopes.contains(&scope) {
            self.scopes.push(scope);
        }
        self
    }

    fn scope_key(&self) -> String {
        self.scopes.join(" ")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Challenge {
    Basic,
    Bearer {
        realm: String,
        service: Option<String>,
    },
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[derive(Default)]
struct AuthState {
    challenges: RwLock<HashMap<String, Challenge>>,
    tokens: RwLock<HashMap<(String, String), CachedToken>>,
    credentials: RwLock<HashMap<(String, String), Credential>>,
}

/// A client for interacting with OCI distribution registries.
///
/// Cloning is cheap; clones share the connection pool and the token cache.
#[derive(Clone)]
pub struct Client {
    pub(crate) http: HttpClient,
    pub(crate) config: ClientConfig,
    metrics: Option<Arc<MetricsCollector>>,
    keychain: Arc<dyn Keychain>,
    auth: Arc<AuthState>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Creates a new client with default configuration and anonymous access.
    pub fn new() -> Result<Self> {
        Self::with_config(ClientConfig::default())
    }

    /// Creates a new client with the given configuration and anonymous access.
    pub fn with_config(config: ClientConfig) -> Result<Self> {
        Self::with_keychain(config, Arc::new(Credential::Anonymous))
    }

    /// Creates a new client that resolves credentials per registry through
    /// `keychain` (e.g. [`containerregistry_auth::default_keychain`]).
    pub fn with_keychain(config: ClientConfig, keychain: Arc<dyn Keychain>) -> Result<Self> {
        let user_agent = match &config.user_agent {
            Some(ua) => format!("{} {}", ua, USER_AGENT_VALUE),
            None => USER_AGENT_VALUE.to_string(),
        };
        let mut builder = HttpClient::builder()
            .connect_timeout(config.timeout)
            .read_timeout(config.timeout)
            .user_agent(user_agent);

        if config.insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }

        Ok(Self {
            http: builder.build()?,
            config,
            metrics: None,
            keychain,
            auth: Arc::new(AuthState::default()),
        })
    }

    /// Creates a new client with metrics collection enabled.
    pub fn with_metrics(config: ClientConfig, metrics: Arc<MetricsCollector>) -> Result<Self> {
        let mut client = Self::with_config(config)?;
        client.metrics = Some(metrics);
        Ok(client)
    }

    /// Creates a new client with a static credential used for every registry.
    pub fn with_credential(config: ClientConfig, credential: Credential) -> Result<Self> {
        Self::with_keychain(config, Arc::new(credential))
    }

    /// Returns a copy of this client that collects metrics into `metrics`.
    pub fn metrics_into(mut self, metrics: Arc<MetricsCollector>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Returns the metrics collector if metrics are enabled.
    pub fn metrics(&self) -> Option<&MetricsCollector> {
        self.metrics.as_ref().map(|m| m.as_ref())
    }

    pub(crate) fn record_success(&self, op: Operation, start: Instant, bytes: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.record_success(op, start.elapsed(), bytes);
        }
    }

    pub(crate) fn record_failure(&self, op: Operation, start: Instant) {
        if let Some(metrics) = &self.metrics {
            metrics.record_failure(op, start.elapsed());
        }
    }

    pub(crate) fn record_upload(&self, op: Operation, start: Instant, bytes: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.record_upload(op, start.elapsed(), bytes);
        }
    }

    fn record_retry(&self, op: Operation) {
        if let Some(metrics) = &self.metrics {
            metrics.record_retry(op);
        }
    }

    /// Records the outcome of an operation.
    pub(crate) fn observe<T>(&self, op: Operation, start: Instant, result: &Result<T>, bytes: u64) {
        match result {
            Ok(_) => self.record_success(op, start, bytes),
            Err(_) => self.record_failure(op, start),
        }
    }

    /// Returns the URL scheme used for `registry`.
    pub fn scheme(&self, registry: &str) -> &'static str {
        if !self.config.https || is_local_registry(registry) {
            "http"
        } else {
            "https"
        }
    }

    /// Builds a URL for the given registry and API path (below `/v2`).
    pub(crate) fn url(&self, registry: &str, path: &str) -> String {
        format!(
            "{}://{}{}{}",
            self.scheme(registry),
            registry,
            API_PREFIX,
            path
        )
    }

    /// Resolves a `Location` header against the registry base URL.
    pub(crate) fn resolve_location(&self, registry: &str, location: &str) -> Result<String> {
        if location.starts_with("http://") || location.starts_with("https://") {
            return Ok(location.to_string());
        }
        let base =
            Url::parse(&format!("{}://{}/", self.scheme(registry), registry)).map_err(|e| {
                Error::InvalidReference(format!("invalid registry {}: {}", registry, e))
            })?;
        base.join(location)
            .map(|u| u.to_string())
            .map_err(|e| Error::UnexpectedStatus {
                status: 0,
                message: format!("invalid Location header {:?}: {}", location, e),
            })
    }

    /// Pings the registry to check if it supports the OCI distribution API.
    ///
    /// Returns `Ok(())` if the registry responds with a 200 OK to `/v2/`
    /// (after completing any authentication challenge).
    pub async fn ping(&self, registry: &str) -> Result<()> {
        let start = Instant::now();
        let url = self.url(registry, "/");
        let result = async {
            let resp = self
                .send(
                    &Target::registry(registry),
                    Some(Operation::Ping),
                    true,
                    || self.http.get(&url),
                )
                .await?;
            match resp.status() {
                StatusCode::OK => Ok(()),
                StatusCode::UNAUTHORIZED => Err(Error::Unauthorized(format!(
                    "registry {} requires authentication",
                    registry
                ))),
                _ => Err(self.error_from_response(resp, "ping", registry).await),
            }
        }
        .await;
        self.observe(Operation::Ping, start, &result, 0);
        result
    }

    /// The `Authorization` header value the client sends for `reference`
    /// with pull (or, with `push`, pull and push) scope plus pull scope on
    /// each repository in `mounts`. The registry's challenge is learned
    /// first; `None` means anonymous access (go `crane auth token`).
    pub async fn authorization_header(
        &self,
        reference: &Reference,
        push: bool,
        mounts: &[String],
    ) -> Result<Option<String>> {
        // Pinging records the registry's challenge; a 401 here is expected.
        let _ = self.ping(reference.registry()).await;
        let mut target = if push {
            Target::push(reference)
        } else {
            Target::pull(reference)
        };
        for m in mounts {
            target = target.with_scope(pull_scope(m));
        }
        self.authorization(&target).await
    }

    // ------------------------------------------------------------------
    // Request pipeline
    // ------------------------------------------------------------------

    /// Sends a request built by `build`, handling authentication challenges
    /// and (when `retry` is true) transient failures.
    ///
    /// `build` is called once per attempt, so its body must be re-creatable
    /// (bytes, not a one-shot stream). Use [`Client::send_once`] for streams.
    pub(crate) async fn send<F>(
        &self,
        target: &Target,
        op: Option<Operation>,
        retry: bool,
        build: F,
    ) -> Result<Response>
    where
        F: Fn() -> RequestBuilder,
    {
        let max_attempts = if retry { self.config.retries } else { 0 };
        let mut attempt = 0u32;
        let mut auth_refreshes = 0u32;

        loop {
            let sent_auth = self.authorization(target).await?;
            let mut rb = build();
            if let Some(value) = &sent_auth {
                rb = rb.header(AUTHORIZATION, value);
            }

            match rb.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::UNAUTHORIZED && auth_refreshes < 2 {
                        self.learn_challenge(target, response.headers(), sent_auth.as_deref())
                            .await?;
                        let new_auth = self.authorization(target).await?;
                        if new_auth.is_some() && new_auth != sent_auth {
                            auth_refreshes += 1;
                            trace!("retrying request with refreshed credentials");
                            continue;
                        }
                        return Ok(response);
                    }

                    if is_retryable_status(status) && attempt < max_attempts {
                        let delay = parse_retry_after(response.headers())
                            .unwrap_or_else(|| self.backoff(attempt));
                        warn!(
                            attempt,
                            status = status.as_u16(),
                            delay_ms = delay.as_millis() as u64,
                            "retryable response, will retry"
                        );
                        if let Some(op) = op {
                            self.record_retry(op);
                        }
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    if attempt > 0 {
                        debug!(attempt, "request completed after retry");
                    }
                    return Ok(response);
                }
                Err(e) => {
                    let err = self.map_transport_error(e);
                    if attempt < max_attempts && err.is_retryable() {
                        trace!(attempt, error = %err, "transport error, will retry");
                        if let Some(op) = op {
                            self.record_retry(op);
                        }
                        let delay = self.backoff(attempt);
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    /// Sends a single request without replaying it (for one-shot streaming
    /// bodies). Authentication should already be established for `target`
    /// by a previous request; a `401` is returned to the caller after the
    /// challenge has been recorded so a retry by the caller can succeed.
    pub(crate) async fn send_once(&self, target: &Target, rb: RequestBuilder) -> Result<Response> {
        let sent_auth = self.authorization(target).await?;
        let rb = match &sent_auth {
            Some(v) => rb.header(AUTHORIZATION, v),
            None => rb,
        };
        let response = rb.send().await.map_err(|e| self.map_transport_error(e))?;
        if response.status() == StatusCode::UNAUTHORIZED {
            self.learn_challenge(target, response.headers(), sent_auth.as_deref())
                .await?;
        }
        Ok(response)
    }

    pub(crate) fn backoff(&self, attempt: u32) -> Duration {
        self.config.retry_backoff * (1u32 << attempt.min(10))
    }

    fn credential(&self, target: &Target) -> Result<Credential> {
        let key = (
            target.registry.clone(),
            target.repository.clone().unwrap_or_default(),
        );
        if let Ok(cache) = self.auth.credentials.read()
            && let Some(c) = cache.get(&key)
        {
            return Ok(c.clone());
        }
        let cred = match &target.repository {
            Some(repo) => self.keychain.resolve_repository(&target.registry, repo)?,
            None => self.keychain.resolve(&target.registry)?,
        };
        if let Ok(mut cache) = self.auth.credentials.write() {
            cache.insert(key, cred.clone());
        }
        Ok(cred)
    }

    /// Computes the `Authorization` header for a request to `target`.
    async fn authorization(&self, target: &Target) -> Result<Option<String>> {
        let cred = self.credential(target)?;
        if let Credential::Bearer(token) = &cred {
            // A registry token is presented as-is (no token exchange).
            return Ok(Some(format!("Bearer {}", token)));
        }

        let challenge = self
            .auth
            .challenges
            .read()
            .ok()
            .and_then(|c| c.get(&target.registry).cloned());

        match challenge {
            Some(Challenge::Bearer { realm, service }) => {
                let key = (target.registry.clone(), target.scope_key());
                let cached = self
                    .auth
                    .tokens
                    .read()
                    .ok()
                    .and_then(|t| t.get(&key).cloned())
                    .filter(|t| t.expires_at > Instant::now());
                if let Some(t) = cached {
                    return Ok(Some(format!("Bearer {}", t.token)));
                }
                // Proactively fetch a token for this scope.
                let token = self
                    .fetch_token(&realm, service.as_deref(), &target.scopes, &cred)
                    .await?;
                let header = format!("Bearer {}", token.token);
                if let Ok(mut tokens) = self.auth.tokens.write() {
                    tokens.insert(key, token);
                }
                Ok(Some(header))
            }
            Some(Challenge::Basic) | None => Ok(cred.authorization_header()),
        }
    }

    /// Records the challenge from a `401` response and, for Bearer
    /// challenges, fetches a fresh token covering the target's scopes plus
    /// any scope the registry asked for.
    async fn learn_challenge(
        &self,
        target: &Target,
        headers: &HeaderMap,
        sent_auth: Option<&str>,
    ) -> Result<()> {
        let Some(parsed) = parse_challenge(headers) else {
            return Ok(());
        };
        match parsed {
            ParsedChallenge::Basic => {
                if let Ok(mut c) = self.auth.challenges.write() {
                    c.insert(target.registry.clone(), Challenge::Basic);
                }
            }
            ParsedChallenge::Bearer {
                realm,
                service,
                scope,
            } => {
                let cred = self.credential(target)?;
                if matches!(cred, Credential::Bearer(_)) {
                    return Ok(());
                }
                if let Ok(mut c) = self.auth.challenges.write() {
                    c.insert(
                        target.registry.clone(),
                        Challenge::Bearer {
                            realm: realm.clone(),
                            service: service.clone(),
                        },
                    );
                }
                let mut scopes = target.scopes.clone();
                for s in scope.iter().flat_map(|s| s.split(' ')) {
                    if !s.is_empty() && !scopes.iter().any(|x| x == s) {
                        scopes.push(s.to_string());
                    }
                }
                let key = (target.registry.clone(), target.scope_key());
                let token = self
                    .fetch_token(&realm, service.as_deref(), &scopes, &cred)
                    .await?;
                if sent_auth == Some(format!("Bearer {}", token.token).as_str()) {
                    // The registry rejected the token it just issued; give up.
                    return Ok(());
                }
                if let Ok(mut tokens) = self.auth.tokens.write() {
                    tokens.insert(key, token);
                }
            }
        }
        Ok(())
    }

    /// Exchanges credentials for a bearer token at `realm`.
    async fn fetch_token(
        &self,
        realm: &str,
        service: Option<&str>,
        scopes: &[String],
        cred: &Credential,
    ) -> Result<CachedToken> {
        #[derive(Deserialize)]
        struct TokenResponse {
            token: Option<String>,
            access_token: Option<String>,
            expires_in: Option<u64>,
        }

        let response = if let Credential::IdentityToken(refresh) = cred {
            // OAuth2 refresh-token grant (Docker identity tokens).
            let mut form = vec![
                ("grant_type", "refresh_token".to_string()),
                ("refresh_token", refresh.clone()),
                ("client_id", "containerregistry-rs".to_string()),
            ];
            if let Some(s) = service {
                form.push(("service", s.to_string()));
            }
            if !scopes.is_empty() {
                form.push(("scope", scopes.join(" ")));
            }
            self.http
                .post(realm)
                .form(&form)
                .send()
                .await
                .map_err(|e| self.map_transport_error(e))?
        } else {
            let mut request = self.http.get(realm);
            if let Some(service) = service {
                request = request.query(&[("service", service)]);
            }
            for scope in scopes {
                request = request.query(&[("scope", scope)]);
            }
            if let Some(header) = cred.authorization_header() {
                request = request.header(AUTHORIZATION, header);
            }
            request
                .send()
                .await
                .map_err(|e| self.map_transport_error(e))?
        };

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Unauthorized(format!(
                "token request to {} failed with status {}: {}",
                realm,
                status.as_u16(),
                body.trim()
            )));
        }
        let body: TokenResponse = response
            .json()
            .await
            .map_err(|e| self.map_transport_error(e))?;
        let token = body
            .token
            .filter(|t| !t.is_empty())
            .or(body.access_token)
            .ok_or_else(|| Error::Unauthorized("token response missing token".to_string()))?;
        // The distribution token spec defaults to 60 seconds.
        let ttl = Duration::from_secs(body.expires_in.unwrap_or(60).max(10));
        Ok(CachedToken {
            token,
            expires_at: Instant::now() + ttl - Duration::from_secs(5),
        })
    }

    // ------------------------------------------------------------------
    // Errors
    // ------------------------------------------------------------------

    /// Converts a non-success response into an [`Error`], decoding the OCI
    /// error body when present.
    pub(crate) async fn error_from_response(
        &self,
        response: Response,
        action: &str,
        target: &str,
    ) -> Error {
        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        let body = response.text().await.unwrap_or_default();
        let details = decode_error_body(&body);
        match status {
            StatusCode::NOT_FOUND => Error::NotFound(target.to_string()),
            StatusCode::UNAUTHORIZED => Error::Unauthorized(match details {
                Some(d) => format!("{}: {}", target, d),
                None => target.to_string(),
            }),
            StatusCode::FORBIDDEN => Error::Forbidden(match details {
                Some(d) => format!("{}: {}", target, d),
                None => target.to_string(),
            }),
            StatusCode::TOO_MANY_REQUESTS => Error::RateLimited {
                retry_after,
                message: format!("rate limited during {} for {}", action, target),
            },
            status => Error::UnexpectedStatus {
                status: status.as_u16(),
                message: match details {
                    Some(d) => format!("{} failed for {}: {}", action, target, d),
                    None => format!("{} failed for {}", action, target),
                },
            },
        }
    }

    /// Maps reqwest errors into semantic error variants.
    pub(crate) fn map_transport_error(&self, e: reqwest::Error) -> Error {
        if e.is_timeout() {
            Error::Timeout {
                duration: self.config.timeout,
                source: e,
            }
        } else if e.is_connect() {
            let message = e.to_string();
            Error::ConnectionFailed { message, source: e }
        } else {
            Error::Transport(e)
        }
    }

    pub(crate) fn parse_digest_header_value(
        header_value: &HeaderValue,
        status: u16,
    ) -> Result<Digest> {
        let header_str = header_value.to_str().map_err(|_| Error::UnexpectedStatus {
            status,
            message: "malformed docker-content-digest header".to_string(),
        })?;
        let header_str = header_str.trim();
        if header_str.is_empty() {
            return Err(Error::UnexpectedStatus {
                status,
                message: "invalid docker-content-digest header: empty".to_string(),
            });
        }
        header_str.parse().map_err(|_| Error::UnexpectedStatus {
            status,
            message: format!("invalid docker-content-digest header: {}", header_str),
        })
    }

    pub(crate) fn parse_optional_digest_header(
        headers: &HeaderMap,
        status: u16,
    ) -> Result<Option<Digest>> {
        match headers.get("docker-content-digest") {
            Some(value) => Ok(Some(Self::parse_digest_header_value(value, status)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn digest_for_algorithm(
        algorithm: Algorithm,
        data: &[u8],
        computed_sha256: &Digest,
    ) -> Digest {
        match algorithm {
            Algorithm::Sha256 => computed_sha256.clone(),
            other => Digest::compute(other, data),
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new().expect("failed to create default client")
    }
}

/// Returns true for registries go-containerregistry talks to over plain HTTP
/// by default: loopback, `localhost`, `*.local`/`*.localhost` and RFC 1918
/// private addresses.
pub fn is_local_registry(registry: &str) -> bool {
    let host = if let Some(rest) = registry.strip_prefix('[') {
        // [::1]:5000
        rest.split(']').next().unwrap_or(rest)
    } else {
        registry.rsplit_once(':').map_or(registry, |(h, port)| {
            if port.chars().all(|c| c.is_ascii_digit()) {
                h
            } else {
                registry
            }
        })
    };
    if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
        return true;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private(),
        Ok(std::net::IpAddr::V6(ip)) => ip.is_loopback(),
        Err(_) => false,
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// Parses Retry-After header value.
///
/// Supports both formats:
/// - Numeric seconds: "120"
/// - HTTP-date: "Wed, 21 Oct 2015 07:28:00 GMT"
pub(crate) fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    let trimmed = value.trim();

    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    if let Ok(date) = httpdate::parse_http_date(trimmed) {
        return Some(
            date.duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO),
        );
    }

    None
}

enum ParsedChallenge {
    Basic,
    Bearer {
        realm: String,
        service: Option<String>,
        scope: Option<String>,
    },
}

fn parse_challenge(headers: &HeaderMap) -> Option<ParsedChallenge> {
    let value = headers.get(WWW_AUTHENTICATE)?.to_str().ok()?.trim();
    let (scheme, rest) = value.split_once(' ').unwrap_or((value, ""));
    if scheme.eq_ignore_ascii_case("basic") {
        return Some(ParsedChallenge::Basic);
    }
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let params = parse_auth_params(rest);
    let realm = params.get("realm")?.clone();
    Some(ParsedChallenge::Bearer {
        realm,
        service: params.get("service").cloned(),
        scope: params.get("scope").cloned(),
    })
}

/// Parses `key="value", key2=value2` auth parameters, honouring quotes (scope
/// values may contain commas, e.g. `repository:foo:pull,push`).
fn parse_auth_params(input: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let mut chars = input.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(c) if *c == ',' || c.is_whitespace()) {
            chars.next();
        }
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                break;
            }
            key.push(c);
            chars.next();
        }
        if chars.next().is_none() {
            break;
        }
        let mut value = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => {
                        if let Some(n) = chars.next() {
                            value.push(n);
                        }
                    }
                    '"' => break,
                    c => value.push(c),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == ',' {
                    break;
                }
                value.push(c);
                chars.next();
            }
        }
        params.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    params
}

/// Decodes an OCI distribution error body (`{"errors":[...]}`) into a
/// human-readable string.
pub(crate) fn decode_error_body(body: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct Body {
        errors: Vec<ApiError>,
    }
    #[derive(Deserialize)]
    struct ApiError {
        #[serde(default)]
        code: String,
        #[serde(default)]
        message: String,
        #[serde(default)]
        detail: Option<serde_json::Value>,
    }
    let parsed: Body = serde_json::from_str(body).ok()?;
    if parsed.errors.is_empty() {
        return None;
    }
    Some(
        parsed
            .errors
            .iter()
            .map(|e| {
                let mut s = if e.message.is_empty() {
                    e.code.clone()
                } else {
                    format!("{}: {}", e.code, e.message)
                };
                if let Some(d) = &e.detail
                    && !d.is_null()
                {
                    s.push_str(&format!(" ({})", d));
                }
                s
            })
            .collect::<Vec<_>>()
            .join("; "),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        assert_eq!(config.timeout, DEFAULT_TIMEOUT);
        assert_eq!(config.retries, DEFAULT_RETRIES);
        assert!(config.https);
        assert!(!config.insecure);
    }

    #[test]
    fn test_client_config_builder() {
        let config = ClientConfig::new()
            .with_timeout(Duration::from_secs(60))
            .with_retries(5)
            .with_https(false)
            .with_insecure(true);

        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.retries, 5);
        assert!(!config.https);
        assert!(config.insecure);
    }

    #[test]
    fn test_url_building() {
        let client = Client::new().unwrap();
        let url = client.url("gcr.io", "/myrepo/manifests/latest");
        assert_eq!(url, "https://gcr.io/v2/myrepo/manifests/latest");
    }

    #[test]
    fn test_url_building_http() {
        let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();
        let url = client.url("localhost:5000", "/myrepo/manifests/latest");
        assert_eq!(url, "http://localhost:5000/v2/myrepo/manifests/latest");
    }

    #[test]
    fn test_local_registries_use_http() {
        for local in [
            "localhost",
            "localhost:5000",
            "127.0.0.1:5000",
            "10.1.2.3",
            "192.168.1.1:443",
            "172.16.0.1",
            "[::1]:5000",
            "registry.local",
        ] {
            assert!(is_local_registry(local), "{local}");
        }
        for remote in [
            "gcr.io",
            "ghcr.io:443",
            "8.8.8.8",
            "172.32.0.1",
            "localhost.com",
        ] {
            assert!(!is_local_registry(remote), "{remote}");
        }
        let client = Client::new().unwrap();
        assert_eq!(client.scheme("localhost:5000"), "http");
        assert_eq!(client.scheme("ghcr.io"), "https");
    }

    #[test]
    fn test_parse_auth_params_with_quoted_commas() {
        let p = parse_auth_params(
            r#"realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:a/b:pull,push repository:c:pull""#,
        );
        assert_eq!(p["realm"], "https://auth.docker.io/token");
        assert_eq!(p["service"], "registry.docker.io");
        assert_eq!(p["scope"], "repository:a/b:pull,push repository:c:pull");
    }

    #[test]
    fn test_decode_error_body() {
        let body = r#"{"errors":[{"code":"MANIFEST_UNKNOWN","message":"manifest unknown","detail":{"Tag":"x"}}]}"#;
        assert_eq!(
            decode_error_body(body).unwrap(),
            r#"MANIFEST_UNKNOWN: manifest unknown ({"Tag":"x"})"#
        );
        assert!(decode_error_body("not json").is_none());
    }

    #[test]
    fn test_resolve_location() {
        let client = Client::with_config(ClientConfig::new().with_https(false)).unwrap();
        assert_eq!(
            client
                .resolve_location("h:5000", "/v2/r/blobs/uploads/u?x=1")
                .unwrap(),
            "http://h:5000/v2/r/blobs/uploads/u?x=1"
        );
        assert_eq!(
            client
                .resolve_location("h:5000", "https://cdn.example/u")
                .unwrap(),
            "https://cdn.example/u"
        );
    }
}
