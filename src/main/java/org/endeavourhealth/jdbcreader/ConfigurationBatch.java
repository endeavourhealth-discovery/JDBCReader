package org.endeavourhealth.jdbcreader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class ConfigurationBatch {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationBatch.class);
    private JsonObject batchObject;
    private List<ConfigurationConnector> connectionList = new ArrayList<ConfigurationConnector>();

    public ConfigurationBatch(JsonObject batchObject) {
        this.batchObject = batchObject;

        JsonArray array = batchObject.getJsonArray("connectorlist");
        for (int i = 0; i < array.size(); i++) {
            JsonObject listitem = array.getJsonObject(i);
            ConfigurationConnector newItem = new ConfigurationConnector(listitem);
            this.connectionList.add(newItem);
            //LOG.trace("Connection config added:" + listitem.toString());
        }
    }

    public String getBatchname() {
        return batchObject.getString("batchname");
    }

    public String getOrganisationId()  {
        return batchObject.getString("organisationId");
    }

    public String getPollFrequency() {
        return batchObject.getString("frequency");
    }

    public String getPollStart() {
        return batchObject.getString("pollstart", "");
    }

    public String getLocalRootPath () {
        return batchObject.getString("localRootPath");
    }

    public String getInterfaceTypeName()  {
        return batchObject.getString("interfaceTypeName");
    }

    public List<ConfigurationConnector> getConnections() {
        return connectionList;
    }
}
