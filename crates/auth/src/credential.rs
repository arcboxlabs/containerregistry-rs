//! Credential types for registry authentication.

use serde::{Deserialize, Serialize};

/// A credential for authenticating to a container registry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Credential {
    /// Anonymous access (no authentication).
    #[default]
    Anonymous,

    /// Basic authentication with username and password.
    Basic { username: String, password: String },

    /// Bearer token authentication.
    Bearer(String),

    /// Identity token (used by some credential helpers).
    IdentityToken(String),
}

impl Credential {
    /// Creates an anonymous credential.
    pub fn anonymous() -> Self {
        Credential::Anonymous
    }

    /// Creates a basic auth credential.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Credential::Basic {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Creates a bearer token credential.
    pub fn bearer(token: impl Into<String>) -> Self {
        Credential::Bearer(token.into())
    }

    /// Creates an identity token credential.
    pub fn identity_token(token: impl Into<String>) -> Self {
        Credential::IdentityToken(token.into())
    }

    /// Returns true if this is anonymous (no auth).
    pub fn is_anonymous(&self) -> bool {
        matches!(self, Credential::Anonymous)
    }

    /// Returns the username if this is basic auth.
    pub fn username(&self) -> Option<&str> {
        match self {
            Credential::Basic { username, .. } => Some(username),
            _ => None,
        }
    }

    /// Returns the password if this is basic auth.
    pub fn password(&self) -> Option<&str> {
        match self {
            Credential::Basic { password, .. } => Some(password),
            _ => None,
        }
    }

    /// Returns the HTTP Authorization header value for this credential.
    pub fn authorization_header(&self) -> Option<String> {
        match self {
            Credential::Anonymous => None,
            Credential::Basic { username, password } => {
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", username, password));
                Some(format!("Basic {}", encoded))
            }
            Credential::Bearer(token) => Some(format!("Bearer {}", token)),
            Credential::IdentityToken(token) => Some(format!("Bearer {}", token)),
        }
    }
}

/// Response from a credential helper's "get" command.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct HelperCredential {
    /// The username.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// The password or secret.
    #[serde(default, rename = "Secret", skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,

    /// The server URL (echoed back by some helpers).
    #[serde(default, rename = "ServerURL", skip_serializing_if = "Option::is_none")]
    pub server_url: Option<String>,
}

impl HelperCredential {
    /// Converts this helper credential to a Credential.
    pub fn into_credential(self) -> Credential {
        match (self.username, self.secret) {
            // Docker's convention for identity tokens stored in helpers.
            (Some(username), Some(secret)) if username == "<token>" && !secret.is_empty() => {
                Credential::identity_token(secret)
            }
            (Some(username), Some(secret)) if !username.is_empty() => {
                Credential::basic(username, secret)
            }
            (_, Some(secret)) if !secret.is_empty() => {
                // No username but has secret - treat as identity token
                Credential::identity_token(secret)
            }
            _ => Credential::Anonymous,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credential_anonymous() {
        let cred = Credential::anonymous();
        assert!(cred.is_anonymous());
        assert_eq!(cred.authorization_header(), None);
    }

    #[test]
    fn test_credential_basic() {
        let cred = Credential::basic("user", "pass");
        assert!(!cred.is_anonymous());
        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass"));

        let header = cred.authorization_header().unwrap();
        assert!(header.starts_with("Basic "));
    }

    #[test]
    fn test_credential_bearer() {
        let cred = Credential::bearer("my-token");
        assert!(!cred.is_anonymous());
        assert_eq!(
            cred.authorization_header(),
            Some("Bearer my-token".to_string())
        );
    }

    #[test]
    fn test_helper_credential_to_credential() {
        let helper = HelperCredential {
            username: Some("user".to_string()),
            secret: Some("pass".to_string()),
            server_url: None,
        };
        let cred = helper.into_credential();
        assert_eq!(cred.username(), Some("user"));
        assert_eq!(cred.password(), Some("pass"));
    }

    #[test]
    fn test_helper_credential_identity_token() {
        let helper = HelperCredential {
            username: None,
            secret: Some("token".to_string()),
            server_url: None,
        };
        let cred = helper.into_credential();
        assert!(matches!(cred, Credential::IdentityToken(_)));
    }
}
