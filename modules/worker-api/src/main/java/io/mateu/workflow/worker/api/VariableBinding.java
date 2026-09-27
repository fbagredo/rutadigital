package io.mateu.workflow.worker.api;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.mateu.workflow.dtos.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Binds the engine's {@code List<Variable>} (name/value string pairs) to and from a task's typed
 * input/output records with Jackson.
 *
 * <p>How a value is read is decided by the <b>declared type</b> of the input property it binds to —
 * the record component generated from the contract's attribute type — never by what the value looks
 * like:
 * <ul>
 *   <li>a textual type ({@code String}, {@code char}, enums, {@code java.time}, {@code UUID}, …;
 *       contract {@code string}, {@code date}, {@code datetime}) gets the raw value unchanged, so a
 *       locator {@code 12E45} or a code {@code 007} stays itself;</li>
 *   <li>any other declared type (numbers, booleans, {@code JsonNode}, lists, maps, nested records;
 *       contract {@code integer}, {@code number}, {@code boolean}, {@code object}, {@code array})
 *       gets the value read as JSON, falling back to the raw string when it is not JSON;</li>
 *   <li>no declared type — {@code Object}, or a {@code Map<String, Object>} input — the raw string.</li>
 * </ul>
 * Variables the input type does not declare are not bound (a task receives every process
 * variable). A binding error is surfaced to the caller as a clear message, which the dispatcher
 * turns into a failed reply.
 */
final class VariableBinding {

    private final ObjectMapper mapper;
    private final Map<Class<?>, DeclaredTypes> declaredTypes = new ConcurrentHashMap<>();

    VariableBinding(ObjectMapper mapper) {
        this.mapper = mapper == null ? WorkerJson.mapper() : mapper;
    }

    /** Build the typed input from the task's variables. Returns null for a no-input handler. */
    <I> I toInput(List<Variable> variables, Class<I> inputType) {
        if (inputType == null || inputType == Void.class) {
            return null;
        }
        var declared = declaredTypes(inputType);
        ObjectNode node = mapper.createObjectNode();
        if (variables != null) {
            for (var variable : variables) {
                var type = declared.typeOf(variable.name());
                if (type == null && !declared.open()) {
                    continue;
                }
                node.set(variable.name(), asNode(variable.value(), type));
            }
        }
        try {
            return mapper.convertValue(node, inputType);
        } catch (IllegalArgumentException e) {
            throw new BindingException("could not read the process variables as "
                    + inputType.getSimpleName() + ": " + rootMessage(e));
        }
    }

    /** Flatten a task's typed output back into variables. */
    List<Variable> toVariables(Object output) {
        if (output == null) {
            return List.of();
        }
        Map<String, Object> map;
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> converted = mapper.convertValue(output, LinkedHashMap.class);
            map = converted;
        } catch (IllegalArgumentException e) {
            throw new BindingException("could not read the task output: " + rootMessage(e));
        }
        var result = new ArrayList<Variable>(map.size());
        map.forEach((name, value) -> result.add(new Variable(name, stringify(value))));
        return result;
    }

    private JsonNode asNode(String value, JavaType declared) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (declared == null || isRawText(declared)) {
            return TextNode.valueOf(value);
        }
        try {
            // A structured or numeric/boolean declared type: read the value as JSON, so an integer,
            // boolean, object or array attribute round-trips. Not JSON → the plain string, which
            // Jackson then coerces or reports as a binding error.
            return mapper.readTree(value);
        } catch (Exception e) {
            return TextNode.valueOf(value);
        }
    }

    /** A declared type that takes the variable's text as it is, rather than parsed as JSON. */
    private static boolean isRawText(JavaType type) {
        var raw = type.getRawClass();
        return raw == Object.class
                || CharSequence.class.isAssignableFrom(raw)
                || raw == char.class || raw == Character.class
                || raw.isEnum()
                || raw == java.util.UUID.class
                || raw.getName().startsWith("java.time.");
    }

    /** The declared property types of an input type, computed once per type. */
    private DeclaredTypes declaredTypes(Class<?> inputType) {
        return declaredTypes.computeIfAbsent(inputType, this::introspect);
    }

    private DeclaredTypes introspect(Class<?> inputType) {
        JavaType type = mapper.constructType(inputType);
        if (type.isMapLikeType()) {
            // A map input declares no attribute types: every variable, as its raw string unless the
            // map's value type says otherwise.
            var valueType = type.getContentType();
            return new DeclaredTypes(Map.of(), true, valueType == null || valueType.getRawClass() == Object.class
                    ? null : valueType);
        }
        if (type.isContainerType() || type.getRawClass() == Object.class
                || JsonNode.class.isAssignableFrom(type.getRawClass())) {
            return new DeclaredTypes(Map.of(), true, null);
        }
        var description = mapper.getDeserializationConfig().introspect(type);
        var types = new LinkedHashMap<String, JavaType>();
        for (var property : description.findProperties()) {
            if (property.couldDeserialize()) {
                types.put(property.getName(), property.getPrimaryType());
            }
        }
        var open = description.findAnySetterAccessor() != null;
        return new DeclaredTypes(types, open, null);
    }

    /**
     * @param byName     the declared type of each property
     * @param open       whether properties it does not declare are bound too (a map, an any-setter)
     * @param otherwise  the type of an undeclared property when open; null for raw text
     */
    private record DeclaredTypes(Map<String, JavaType> byName, boolean open, JavaType otherwise) {
        JavaType typeOf(String name) {
            var declared = byName.get(name);
            return declared != null ? declared : otherwise;
        }
    }

    private String stringify(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private static String rootMessage(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage();
    }

    /** A binding failure — turned into a failed reply with a clear reason by the dispatcher. */
    static final class BindingException extends RuntimeException {
        BindingException(String message) {
            super(message);
        }
    }
}
