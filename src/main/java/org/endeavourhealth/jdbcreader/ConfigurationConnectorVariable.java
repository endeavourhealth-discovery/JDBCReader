package org.endeavourhealth.jdbcreader;

import javax.json.JsonObject;

public class ConfigurationConnectorVariable {
    private JsonObject variableObject;

    public ConfigurationConnectorVariable(JsonObject variableObject) {
        this.variableObject = variableObject;
    }

    public String getName() {
        return variableObject.getString("name");
    }

    public String getType() {
        return variableObject.getString("type");
    }

    public String getValue() {
        return variableObject.getString("value");
    }

    public long getValueAsLong() {
        return Long.parseLong(variableObject.getString("value"));
    }


}
