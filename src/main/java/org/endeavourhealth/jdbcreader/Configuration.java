package org.endeavourhealth.jdbcreader;

import com.fasterxml.jackson.databind.JsonNode;
import com.kstruct.gethostname4j.Hostname;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.jdbcreader.utilities.JDBCReaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import javax.json.Json;
//import javax.json.JsonArray;
//import javax.json.JsonObject;
//import javax.json.JsonReader;
//import javax.sql.DataSource;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class Configuration {

    // class members //
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    private static final String PROGRAM_CONFIG_MANAGER_NAME = "jdbcreader";
    private static final String INSTANCE_NAME_JAVA_PROPERTY = "INSTANCE_NAME";
    // instance members //
    private List<JsonNode> interfaceFileTypes = new ArrayList<JsonNode>() ;
    private String machineName;
    private String instanceName;
    private JsonNode objroot;
    private List<ConfigurationBatch> batchConfigurationList = new ArrayList<ConfigurationBatch>();

    public Configuration(JsonNode root) throws Exception {
        this.objroot = root;
        //LOG.info("configuration:" + objroot.toString());
        //LOG.info("just checking:" + getConfigurationId());
        initialiseMachineName();
        retrieveInstanceName();
        //loadDbConfiguration();
        JsonNode fileTypeArray = objroot.path("interfaceFileTypes");
        if (fileTypeArray.isMissingNode() != true && fileTypeArray.isArray()) {
            for (JsonNode node : fileTypeArray) {
                interfaceFileTypes.add(node);
            }
        }

        JsonNode batchList = objroot.path("batchlist");
        //LOG.info("batchlist:" + batchList.isArray());
        //LOG.info("batchlist:" + batchList.isMissingNode());
        if (batchList.isMissingNode() != true && batchList.isArray()) {
            for (JsonNode node : batchList) {
                ConfigurationBatch newItem = new ConfigurationBatch(node);
                batchConfigurationList.add(newItem);
            }
        }
    }

    public String getMachineName() { return machineName; }
    public String getInstanceName() { return instanceName; }
    public List<ConfigurationBatch> getBatchConfigurations() { return this.batchConfigurationList; }
    public String getConfigurationId() {
        return objroot.get("configurationId").asText();
    }

    public String getDestinationPathPrefix() {
        return objroot.get("destinationPathPrefix").asText();
    }
    public String getTempPathPrefix() {
        return objroot.get("tempPathPrefix").asText();
    }

    /*
    public String getSoftwareContentType() {
        return objroot.get("softwareContentType").asText();
    }*/
    public String getConfigurationFriendlyName() {
        return objroot.get("configurationFriendlyName").asText();
    }

    /*
    public String getSoftwareVersion() {
        return objroot.get("softwareVersion").asText();
    }

    public int getHttpManagementPort() {
        return objroot.get("HttpManagementPort").asInt();
    } */

    public boolean notifyStartStopSlack() {
        JsonNode jn = objroot.get("notifyStartStopSlack");
        if (jn != null) {
            return jn.asBoolean(false);
        } else {
            return false;
        }
    }

    /*
    public List<JsonNode> getInterfaceFileTypes() {
        return interfaceFileTypes;
    }*/

    private void initialiseMachineName() throws JDBCReaderException {
        try {
            machineName = Hostname.getHostname();
        } catch (Exception e) {
            throw new JDBCReaderException("Error getting machine name", e);
        }
    }

    private void retrieveInstanceName() throws JDBCReaderException {
        try {
            this.instanceName = System.getProperty(INSTANCE_NAME_JAVA_PROPERTY);

            if (StringUtils.isEmpty(this.instanceName))
                throw new JDBCReaderException("Could not find " + INSTANCE_NAME_JAVA_PROPERTY + " Java -D property");

        } catch (JDBCReaderException e) {
            throw e;
        } catch (Exception e) {
            throw new JDBCReaderException("Could not read " + INSTANCE_NAME_JAVA_PROPERTY + " Java -D property");
        }
    }

    public ConfigurationBatch getBatchConfiguration(String configurationId) throws JDBCReaderException {
        List<ConfigurationBatch> dbConfigurations = this.batchConfigurationList
                .stream()
                .filter(t -> t.getBatchname().equals(configurationId))
                .collect(Collectors.toList());

        if (dbConfigurations.size() == 0)
            throw new JDBCReaderException("Could not find configuration with id " + configurationId);

        if (dbConfigurations.size() > 1)
            throw new JDBCReaderException("Multiple configurations found with id " + configurationId);

        return dbConfigurations.get(0);
    }

}
