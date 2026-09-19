package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AuthConfig;
import io.jonasg.kawa.config.ClientConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.config.GroupConfig;
import io.jonasg.kawa.config.RbacConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/// Serves `/config/auth/clients`: lists the clients, upserts one entry via
/// `PUT /config/auth/clients/{name}` and removes it via `DELETE /config/auth/clients/{name}`.
/// Each write persists a full [GatewayConfig] snapshot through the [GatewayConfigRepository].
/// Adding a client auto-expands the advertised SASL mechanisms to include the client's
/// mechanism, so the first client can be added to an empty config.
public final class AuthClientsConfigHandler extends ConfigSectionHandler<ClientConfig> {

    public AuthClientsConfigHandler(GatewayConfigRepository repository) {
        super(repository, ClientConfig.class, "client");
    }

    @Override
    protected Map<String, ClientConfig> entries(GatewayConfig config) {
        return config.auth().clients();
    }

    @Override
    protected Object listView(GatewayConfig config) {
        return entries(config).entrySet().stream()
                .map(entry -> new ClientView(entry.getKey(), entry.getValue().mechanism()))
                .sorted(Comparator.comparing(ClientView::username))
                .toList();
    }

    @Override
    protected GatewayConfig upsert(GatewayConfig config, String name, ClientConfig value) {
        AuthConfig auth = config.auth().withClient(name, value);
        return config.updateAuth(auth);
    }

    @Override
    protected GatewayConfig remove(GatewayConfig config, String name) {
        return config.updateAuth(config.auth().removeClient(name));
    }

    @Override
    protected Router.Response<?> validateRemove(GatewayConfig config, String name) {
        List<String> groups = config.rbac().groups().entrySet().stream()
                .filter(entry -> entry.getValue().clients().contains(name))
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
        if (groups.isEmpty()) {
            return null;
        }
        return Router.Response.conflict("client '" + name + "' is still in groups " + groups);
    }

    /// `PATCH` updates one or both credential fields of an existing client; a field left out
    /// of the body keeps its stored value. The password stays write-only — it is never
    /// returned.
    @Override
    public Router.Response<?> handle(Router.Request request) {
        if (!"PATCH".equals(request.method()) && !"PUT".equals(request.method())) {
            return super.handle(request);
        }
        GatewayConfig base = repository.getActiveConfigOrEmpty();
        String name = request.pathParams().get("name");
        ClientConfig current = entries(base).get(name);
        boolean patchRequest = "PATCH".equals(request.method());
        ClientConfigRequest body;
        try {
            body = patchRequest
                    ? mapper.readValue(request.body(), ClientConfigPatch.class).toRequest()
                    : mapper.readValue(request.body(), ClientConfigRequest.class);
        } catch (Exception e) {
            return Router.Response.badRequest("invalid client body: " + e.getMessage());
        }
        if (patchRequest && current == null) {
            return Router.Response.notFound("client '" + name + "' not found");
        }
        if (patchRequest && body.mechanism() == null && body.password() == null && body.groups() == null) {
            return Router.Response.badRequest("no fields to patch");
        }
        String mechanism = body.mechanism() != null || current == null
                ? body.mechanism() : current.mechanism();
        String password = body.password() != null || current == null
                ? body.password() : current.password();
        List<String> groups = patchRequest || body.groups() != null ? body.groups() : List.of();
        try {
            updater.update(request, config -> updateClient(
                    config, name, new ClientConfig(mechanism, password), groups));
        } catch (IllegalArgumentException e) {
            return Router.Response.badRequest(e.getMessage());
        }
        return patchRequest
                ? Router.Response.ok(new ClientView(name, mechanism))
                : Router.Response.ok(new ClientConfig(mechanism, password));
    }

    private GatewayConfig updateClient(
            GatewayConfig config,
            String name,
            ClientConfig client,
            List<String> groupNames
    ) {
        if (groupNames == null) {
            groupNames = groupsForClient(config, name);
        }
        Set<String> selectedGroups = new HashSet<>(groupNames);
        for (String groupName : selectedGroups) {
            if (!config.rbac().groups().containsKey(groupName)) {
                throw new IllegalArgumentException("group '" + groupName + "' not found");
            }
        }
        var groups = config.rbac().groups().entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            List<String> clients = entry.getValue().clients().stream()
                                    .filter(clientName -> !clientName.equals(name))
                                    .collect(Collectors.toCollection(ArrayList::new));
                            if (selectedGroups.contains(entry.getKey())) {
                                clients.add(name);
                            }
                            return new GroupConfig(clients, entry.getValue().roles());
                        }));
        return config.updateAuth(config.auth().withClient(name, client))
                .updateRbac(new RbacConfig(config.rbac().roles(), groups));
    }

    private List<String> groupsForClient(GatewayConfig config, String name) {
        return config.rbac().groups().entrySet().stream()
                .filter(entry -> entry.getValue().clients().contains(name))
                .map(Map.Entry::getKey)
                .toList();
    }
}
