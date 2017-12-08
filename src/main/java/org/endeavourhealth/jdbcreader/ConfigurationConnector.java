package org.endeavourhealth.jdbcreader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class ConfigurationConnector {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationConnector.class);

    private JsonObject connectionObject;
    private List<ConfigurationConnectorVariable> variableList = new ArrayList<ConfigurationConnectorVariable>();


    public ConfigurationConnector(JsonObject connectionObject) {
        this.connectionObject = connectionObject;

        JsonArray array = connectionObject.getJsonArray("variablelist");
        if (array != null) {
            for (int i = 0; i < array.size(); i++) {
                JsonObject listitem = array.getJsonObject(i);
                ConfigurationConnectorVariable newItem = new ConfigurationConnectorVariable(listitem);
                this.variableList.add(newItem);
                //LOG.trace("Connection variable added:" + listitem.toString());
            }
        }
    }

    public String getDriver() {
        return connectionObject.getString("sqldriver");
    }

    public String getSqlStatement() {
        return connectionObject.getString("sqlstatement");
    }

    public String getSqlURL() {
        return connectionObject.getString("sqlurl");
    }

    public String getConnectorName() {
        return connectionObject.getString("connectorname");
    }

    public String getInterfaceFileType() {
        return connectionObject.getString("interfaceFileType");
    }

    public boolean isNullValueAsString() {
        return connectionObject.getBoolean("nullValueAsString");
    }

    public List<ConfigurationConnectorVariable> getVariables() {
        return variableList;
    }
}
