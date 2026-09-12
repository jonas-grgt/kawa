package io.jonasg.kawa.http;

/// A single auth user in the admin `/auth/users` response.
///
/// @param username the SASL username
/// @param mechanism the SASL mechanism (e.g. PLAIN, SCRAM-SHA-256)
/// @param password the password; may reference environment variables as `${VAR}` or `${VAR:-default}`
public record UserView(
        String username,
        String mechanism,
        String password) {
}