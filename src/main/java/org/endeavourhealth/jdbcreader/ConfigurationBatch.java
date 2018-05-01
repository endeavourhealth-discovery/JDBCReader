package org.endeavourhealth.jdbcreader;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ConfigurationBatch {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationBatch.class);
    private JsonNode batchObject;
    private List<ConfigurationConnector> connectionList = new ArrayList<ConfigurationConnector>();

    public ConfigurationBatch(JsonNode batchObject) {
        this.batchObject = batchObject;

        JsonNode array = batchObject.path("connectorlist");
        if (array.isMissingNode() != true && array.isArray()) {
            for (JsonNode node : array) {
                ConfigurationConnector newItem = new ConfigurationConnector(node);
                this.connectionList.add(newItem);
            }
        }
    }

    public String getBatchname() {
        return batchObject.get("batchname").asText();
    }

    /*
    public String getOrganisationId()  {
        return batchObject.get("organisationId").asText();
    }*/

    public String getPollFrequency() {
        return batchObject.get("frequency").asText();
    }

    public String getPollStart() {
        return (batchObject.get("pollstart") == null ? "" : batchObject.get("pollstart").asText());
    }

    /*
    public String getLocalRootPath () {
        return batchObject.get("localRootPath").asText();
    }

    public String getInterfaceTypeName()  {
        return batchObject.get("interfaceTypeName").asText();
    }*/

    public boolean zipDestinationFile()  {
        return (batchObject.get("zipDestinationFile") == null ? false : batchObject.get("zipDestinationFile").asBoolean(false));
    }

    public boolean isActive()  {
        return (batchObject.get("active") == null ? true : batchObject.get("active").asBoolean(true));
    }

    public boolean removeTempFile()  {
        return (batchObject.get("removeTempFile") == null ? true : batchObject.get("removeTempFile").asBoolean(true));
    }

    public List<ConfigurationConnector> getConnections() {
        return connectionList;
    }
}
