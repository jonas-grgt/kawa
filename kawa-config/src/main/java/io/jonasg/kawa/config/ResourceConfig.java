package io.jonasg.kawa.config;

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

/// The resource an ACL applies to. `pattern` is required for [ResourceType#TOPIC] and
/// [ResourceType#GROUP] but must be `null` for [ResourceType#CLUSTER], which matches the
/// cluster regardless of name. Pattern type defaults to [PatternType#LITERAL] when omitted.
///
/// A [PatternType#PREFIXED] resource may carry an empty `pattern`, which matches every
/// resource of that type - the sanctioned way to express "any TOPIC" or "any GROUP".
///
/// @param type the resource type
/// @param pattern the resource name, or `null` for [ResourceType#CLUSTER]
/// @param patternType how `pattern` is matched
public record ResourceConfig(
        ResourceType type,
        String pattern,
        PatternType patternType
) {

    /// Convenience constructor for a [PatternType#LITERAL] resource - the common case, matching
    /// the YAML default where `patternType` is omitted.
    public ResourceConfig(ResourceType type, String pattern) {
        this(type, pattern, PatternType.LITERAL);
    }

    public ResourceConfig {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (patternType == null) {
            patternType = PatternType.LITERAL;
        }
        if (type == ResourceType.CLUSTER) {
            pattern = null;
        } else if (pattern == null || (pattern.isBlank() && patternType != PatternType.PREFIXED)) {
            throw new IllegalArgumentException(
                    "pattern must not be null or blank for resource type " + type);
        }
    }
}
