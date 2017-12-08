package org.endeavourhealth.jdbcreader;

import com.kstruct.gethostname4j.Hostname;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.jdbcreader.utilities.JDBCReaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.sql.DataSource;
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
    private List<String> interfaceFileTypes = new ArrayList<String>() ;
    private String primaryConfig;
    private String machineName;
    private String instanceName;
    private JsonObject objroot;
    private List<ConfigurationBatch> batchConfigurationList = new ArrayList<ConfigurationBatch>();

    public Configuration(String primaryConfig) throws Exception {
        this.primaryConfig = primaryConfig;
        initialiseMachineName();
        retrieveInstanceName();
        loadDbConfiguration();
    }

    public String getMachineName() { return machineName; }
    public String getInstanceName() { return instanceName; }
    public List<ConfigurationBatch> getBatchConfigurations() { return this.batchConfigurationList; }
    public String getConfigurationId() {
        return objroot.getString("configurationId");
    }

    public String getLocalRootPathPrefix() {
        return objroot.getString("localRootPathPrefix");
    }
    public String getSoftwareContentType() {
        return objroot.getString("softwareContentType");
    }
    public String getConfigurationFriendlyName() {
        return objroot.getString("configurationFriendlyName");
    }

    public String getSoftwareVersion() {
        return objroot.getString("softwareVersion");
    }
    public String getEdsUrl() {
        return objroot.getString("edsurl");
    }
    public boolean isUseKeycloak() {
        return objroot.getBoolean("usekeycloak");
    }
    public String getKeycloakTokenUri() {
        return objroot.getString("keycloaktokenuri");
    }
    public String getKeycloakRealm() {
        return objroot.getString("keycloakrealm");
    }
    public String getKeycloakUsername() {
        return objroot.getString("keycloakusername");
    }
    public String getKeycloakPassword() {
        return objroot.getString("keycloakpassword");
    }
    public String getKeycloakClientId() {
        return objroot.getString("keycloakclientid");
    }

    public int getHttpManagementPort() {
        return objroot.getInt("HttpManagementPort");
    }

    public List<String> getInterfaceFileTypes() {
        return interfaceFileTypes;
    }

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

    private void loadDbConfiguration() throws SQLException, JDBCReaderException {
        JsonArray array;
        JsonReader rdr = Json.createReader(new StringReader(primaryConfig));
        JsonObject obj = rdr.readObject();
        objroot = obj.getJsonObject("connectorconfiguration");

        array = objroot.getJsonArray("interfaceFileTypes");
        for (int i = 0; i < array.size(); i++) {
            interfaceFileTypes.add(array.getString(i));
        }

        array = objroot.getJsonArray("batchlist");
        for (int i = 0; i < array.size(); i++) {
            JsonObject listitem = array.getJsonObject(i);
            ConfigurationBatch newItem = new ConfigurationBatch(listitem);
            this.batchConfigurationList.add(newItem);
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
