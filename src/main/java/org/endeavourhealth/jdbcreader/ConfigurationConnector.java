package org.endeavourhealth.jdbcreader;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import javax.json.JsonArray;
//import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class ConfigurationConnector {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationConnector.class);

    //private JsonObject connectionObject;
    private JsonNode connectionObject;
    private List<ConfigurationConnectorVariable> variableList = new ArrayList<ConfigurationConnectorVariable>();


    public ConfigurationConnector(JsonNode connectionObject) {
        this.connectionObject = connectionObject;

        JsonNode array = connectionObject.path("variablelist");

        if (array.isMissingNode() != true && array.isArray()) {
            for (JsonNode node : array) {
                ConfigurationConnectorVariable newItem = new ConfigurationConnectorVariable(node);
                this.variableList.add(newItem);
            }
        }
    }

    public String getDriver() {
        return connectionObject.get("sqldriver").asText();
    }

    public String getSqlStatement() {
        return connectionObject.get("sqlstatement").asText();
    }

    public String getSqlURL() {
        return connectionObject.get("sqlurl").asText();
    }

    public String getConnectorName() {
        return connectionObject.get("connectorname").asText();
    }

    /*
    public String getInterfaceFileType() {
        return connectionObject.get("interfaceFileType").asText();
    }*/

    public String getFilename() {
        return connectionObject.get("filename").asText();
    }

    public boolean writeEmptyFileIfNothingFound() {
        return (connectionObject.get("writeEmptyFileIfNothingFound") == null ? false : connectionObject.get("writeEmptyFileIfNothingFound").asBoolean(false));
    }

    public boolean isActive()  {
        return (connectionObject.get("active") == null ? true : connectionObject.get("active").asBoolean(true));
    }

    public boolean isNullValueAsString() {
        return (connectionObject.get("nullValueAsString") == null ? true : connectionObject.get("nullValueAsString").asBoolean(true));
    }

    public List<ConfigurationConnectorVariable> getVariables() {
        return variableList;
    }
}
