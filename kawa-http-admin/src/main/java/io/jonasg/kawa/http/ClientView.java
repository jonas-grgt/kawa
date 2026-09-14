package io.jonasg.kawa.http;

/// A single auth client in the admin `/auth/clients` response.
///
/// @param username the SASL username
/// @param mechanism the SASL mechanism (e.g. PLAIN, SCRAM-SHA-256)
/// @param password the password; may reference environment variables as `${VAR}` or `${VAR:-default}`
public record ClientView(
        String username,
        String mechanism
) { }