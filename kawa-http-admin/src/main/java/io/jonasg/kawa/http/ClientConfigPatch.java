package io.jonasg.kawa.http;

/// A partial update for a single auth client. Fields not supplied are preserved
/// as-is; at least one of `mechanism` or `password` must be present.
///
/// @param mechanism the new SASL mechanism, or `null` to keep the current one
/// @param password  the new password, or `null` to keep the current one
public record ClientConfigPatch(
        String mechanism,
        String password
) {
}
