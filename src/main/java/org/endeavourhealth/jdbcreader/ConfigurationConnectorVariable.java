package org.endeavourhealth.jdbcreader;

import com.fasterxml.jackson.databind.JsonNode;

public class ConfigurationConnectorVariable {
    private JsonNode variableObject;

    public ConfigurationConnectorVariable(JsonNode variableObject) {
        this.variableObject = variableObject;
    }

    public String getName() {
        return variableObject.get("name").asText();
    }

    public String getType() {
        return variableObject.get("type").asText();
    }

    public String getValue() {
        return variableObject.get("value").asText();
    }

    public long getValueAsLong() {
        return variableObject.get("value").asLong();
    }


}
