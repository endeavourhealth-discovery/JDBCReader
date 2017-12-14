package org.endeavourhealth.jdbcreader;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.endeavourhealth.common.eds.EdsSender;
import org.endeavourhealth.common.eds.EdsSenderHttpErrorResponseException;
import org.endeavourhealth.common.eds.EdsSenderResponse;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;
import org.endeavourhealth.core.database.dal.jdbcreader.models.BatchFile;
import org.endeavourhealth.core.database.dal.jdbcreader.models.KeyValuePair;
import org.endeavourhealth.core.database.dal.jdbcreader.models.NotificationMessage;
import org.endeavourhealth.jdbcreader.implementations.*;
import org.endeavourhealth.jdbcreader.utilities.JDBCReaderException;
import org.endeavourhealth.jdbcreader.utilities.JDBCValidationException;
import org.endeavourhealth.jdbcreader.utilities.SlackNotifier;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcReaderTask implements Runnable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JdbcReaderTask.class);
    private Map<Long, String> notificationErrorrs = new HashMap<>();
    private BufferedWriter dataLogFileBuffer = null;

    private Configuration configuration = null;
    private String configurationId = null;
    private ConfigurationBatch configurationBatch = null;
    private DataLayer db = null;

    public JdbcReaderTask(Configuration configuration, String configurationId) {
        this.configuration = configuration;
        this.configurationId = configurationId;
        db = new DataLayer();
    }

    @Override
    public void run() {
        try {
            LOG.trace(">>>Starting scheduled JDBCReader run, initialising");
            initialise();

            // Get last batch
            Batch currentbatch = getOrCreateBatch();
            LOG.trace("Current BatchId:" + currentbatch.getBatchId());

            LOG.trace(">>>Retrieving data");
            if (!downloadAndProcessFiles(currentbatch)) {
                throw new JDBCReaderException("Exception occurred downloading and processing files - halting to prevent incorrect ordering of batches.");
            }

            LOG.trace(">>>Validating batches");
            if (!validateBatches()) {
                throw new JDBCReaderException("Exception occurred validating batches - halting to prevent incorrect ordering of batches.");
            }

            LOG.trace(">>>Notifying EDS");
            notifyEds();

            LOG.trace(">>>Completed JDBCReader run");

        } catch (Exception e) {
            LOG.error(">>>Fatal exception in JDBCTask run, terminating this run", e);
        }
    }

    /*
     * Get last Batch record for this batch name
     */
    private Batch getOrCreateBatch() throws Exception {
        Batch currentbatch = db.getLastBatch(configuration.getConfigurationId(), configurationBatch.getBatchname());
        if (currentbatch == null) {
            // First time for this batch
            LOG.trace("No Batch record found for " + configurationBatch.getBatchname());
            Batch newbatch = new Batch();
            newbatch.setConfigurationId(configuration.getConfigurationId());
            newbatch.setInterfaceTypeName(configurationBatch.getInterfaceTypeName());
            newbatch.setBatchIdentifier(configurationBatch.getBatchname() + "#1");
            newbatch.setInsertDate(new Date());
            newbatch.setComplete(false);
            db.addBatch(newbatch);
            return newbatch;
        } else if (currentbatch.isComplete()) {
            // Last batch is complete - allocate new batch number
            LOG.trace("Complete Batch record found for " + configurationBatch.getBatchname() + " Identifier is " + currentbatch.getBatchIdentifier());
            Batch newbatch = new Batch();
            newbatch.setConfigurationId(configuration.getConfigurationId());
            newbatch.setInterfaceTypeName(configurationBatch.getInterfaceTypeName());
            int currBatchSeq = Integer.parseInt(currentbatch.getBatchIdentifier().split("#")[1]);
            newbatch.setBatchIdentifier(configurationBatch.getBatchname() + "#" + ++currBatchSeq);
            newbatch.setInsertDate(new Date());
            newbatch.setComplete(false);
            db.addBatch(newbatch);
            return newbatch;
        } else {
            // Last batch is incomplete - continue
            LOG.trace("Batch record is incomplete for " + configurationBatch.getBatchname());
            return currentbatch;
        }
    }
    /*
     *
     */
    private void initialise() throws Exception {
        this.configurationBatch = configuration.getBatchConfiguration(configurationId);
        checkLocalRootPathPrefixExists();
    }

    /*
     *
     */
    private void checkLocalRootPathPrefixExists() throws Exception {
        if (StringUtils.isNotEmpty(this.configuration.getLocalRootPathPrefix())) {

            File rootPath = new File(this.configuration.getLocalRootPathPrefix());

            if ((!rootPath.exists()) || (!rootPath.isDirectory()))
                throw new JDBCReaderException("Local root path prefix '" + rootPath + "' does not exist");
        }
    }

    /*
     *
     */
    private boolean downloadAndProcessFiles(Batch currentBatch) {
        HashMap<String, Connection> connectionList = new HashMap<String, Connection>();

        try {
            LOG.trace("Found {} connections in batch {}", configurationBatch.getConnections().size(), configurationBatch.getBatchname());

            int countAlreadyProcessed = 0;

            // Loop through all connections for this batch
            for (ConfigurationConnector configurationConnector : configurationBatch.getConnections()) {
                LOG.trace("Processing connector " + configurationConnector.getConnectorName());

                // Get previously saved key-value-pairs
                List<KeyValuePair> kvpListDB = db.getKeyValuePairs(configurationBatch.getBatchname(), configurationConnector.getConnectorName());
                HashMap<String, String> kvpList = new HashMap<String, String>();
                for (KeyValuePair kvp : kvpListDB) {
                    kvpList.put(kvp.getKeyValue(), kvp.getDataValue());
                }
                LOG.trace("Found " + kvpList.size() + " KVP entries");

                // Check for variables
                if (configurationConnector.getVariables().size() > 0) {
                    adjustKVPListForVariableSettings(kvpList, configurationConnector.getVariables());
                }

                // Re-use connection or open new
                Connection connection = null;
                if (connectionList.containsKey(configurationConnector.getSqlURL())) {
                    LOG.trace("Reusing existing connection");
                    connection = connectionList.get(configurationConnector.getSqlURL());
                } else {
                    LOG.trace("Creating new connection");
                    try {
                        connection = DriverManager.getConnection(configurationConnector.getSqlURL());
                    }
                    catch (SQLException e) {
                        LOG.trace("SQLException - Auto load of driver not working");
                        LOG.trace( "Loading driver (" + configurationConnector.getDriver() + ") .....");
                        Class.forName(configurationConnector.getDriver());
                        LOG.trace( "......Driver loaded - trying again");
                        connection = DriverManager.getConnection(configurationConnector.getSqlURL());
                    }
                }

                LocalDataFile localDataFile = new LocalDataFile();
                localDataFile.setLocalRootPathPrefix(configuration.getLocalRootPathPrefix());
                localDataFile.setLocalRootPath(configurationBatch.getLocalRootPath());
                localDataFile.setBatchIdentifier(currentBatch.getBatchIdentifier());
                localDataFile.setFileName(configurationConnector.getInterfaceFileType() + ".csv");

                createBatchDirectory(localDataFile);

                currentBatch.setLocalPath(localDataFile.getLocalPathBatch());

                BatchFile batchFile = db.getBatchFile(currentBatch.getBatchId(), localDataFile.getFileName());
                if (batchFile == null) {
                    // First attempt on this file / sql-statement
                    LOG.trace("BatchFile record not found for " + localDataFile.getFileName());
                    batchFile = new BatchFile();
                    batchFile.setBatchId(currentBatch.getBatchId());
                    batchFile.setFileTypeIdentifier(configurationConnector.getInterfaceFileType());
                    batchFile.setInsertDate(new Date());
                    batchFile.setFilename(localDataFile.getFileName());
                    db.addBatchFile(batchFile);
                } else if (batchFile.isDownloaded()) {
                    // File already downloaded - SQL content already saved successfully to local file
                    LOG.trace("BatchFile record found for " + localDataFile.getFileName() + " and already marked as downloaded");
                    countAlreadyProcessed ++;
                    continue;
                } else {
                    // Retry
                    LOG.trace("BatchFile record found for " + localDataFile.getFileName() + " and NOT marked as downloaded");
                }
                LOG.trace("Current BatchFileId:" + batchFile.getBatchFileId());

                getData(connection, localDataFile, configurationConnector, kvpList);

                db.setBatchFileAsDownloaded(batchFile);

                LOG.trace("Saving " + kvpList.size() + " KVP entries");
                db.insertUpdateKeyValuePair(configurationBatch.getBatchname(), configurationConnector.getConnectorName(), kvpList);
            }

            if (countAlreadyProcessed > 0)
                LOG.trace("Skipped {} files as already processed them", new Integer(countAlreadyProcessed));

            db.setBatchAsComplete(currentBatch);

            //LOG.info("Completed processing {} files", Integer.toString(remoteFiles.size()));

            return true;
        } catch (Exception e) {
            LOG.error("Exception occurred while processing files - cannot continue or may process batches out of order", e);
        } finally {
            connectionList.forEach((k, c) -> {
                try {
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        return false;
    }

    /*
     *
     */
    private void adjustKVPListForVariableSettings(HashMap<String, String> kvpList, List<ConfigurationConnectorVariable> variableList) {
        for (ConfigurationConnectorVariable ccv : variableList) {
            String kvpValue = kvpList.getOrDefault(ccv.getName(), null);
            if (ccv.getType().compareToIgnoreCase("initialvalue") == 0) {
                // Any value - streated as string
                if (kvpValue == null) {
                    kvpList.put(ccv.getName(), ccv.getValue());
                } else {
                    // Value already set
                }
            } else if (ccv.getType().compareToIgnoreCase("dayincrementer") == 0) {
                // Date must be YYYY-MM-DD or YYYYMMDD
                if (kvpValue == null) {
                    kvpList.put(ccv.getName(), ccv.getValue());
                } else {
                    // value already set - increment by 1 day
                    String componentDelim;
                    LocalDateTime ldt;
                    if (kvpValue.indexOf("-") > 0) {
                        componentDelim = "-";
                        ldt = LocalDateTime.of(Integer.parseInt(kvpValue.split("-")[0]), Integer.parseInt(kvpValue.split("-")[1]), Integer.parseInt(kvpValue.split("-")[2]), 0, 0);
                    } else {
                        componentDelim = "";
                        ldt = LocalDateTime.of(Integer.parseInt(kvpValue.substring(0, 4)), Integer.parseInt(kvpValue.substring(4, 6)), Integer.parseInt(kvpValue.substring(6)), 0, 0);
                    }
                    ldt = ldt.plusDays(1);
                    StringBuffer sb = new StringBuffer();
                    sb.append(ldt.getYear());
                    sb.append(componentDelim);
                    sb.append((ldt.getMonthValue() < 10 ? "0" + ldt.getMonthValue() : ldt.getMonthValue()));
                    sb.append(componentDelim);
                    sb.append((ldt.getDayOfMonth() < 10 ? "0" + ldt.getDayOfMonth() : ldt.getDayOfMonth()));
                    kvpList.replace(ccv.getName(), sb.toString());
                }
            } else if (ccv.getType().compareToIgnoreCase("hourincrementer") == 0) {
                //Time must be hh:mm
                if (kvpValue == null) {
                    kvpList.put(ccv.getName(), ccv.getValue());
                } else {
                    // value already set - increment by 1 hour
                    LocalDateTime ldt = LocalDateTime.of(1, 1, 1, Integer.parseInt(kvpValue.split(":")[0]), Integer.parseInt(kvpValue.split(":")[1]));
                    ldt = ldt.plusHours(1);
                    StringBuffer sb = new StringBuffer();
                    sb.append((ldt.getHour() < 10 ? "0" + ldt.getHour() : ldt.getHour()));
                    sb.append(":" + (ldt.getMinute() < 10 ? "0" + ldt.getMinute() : ldt.getMinute()));
                    kvpList.replace(ccv.getName(), sb.toString());
                }
            } else if (ccv.getType().compareToIgnoreCase("numberincrementer") == 0) {
                // Value must be numeric and compatible with java type long
                if (kvpValue == null) {
                    kvpList.put(ccv.getName(), Long.toString(ccv.getValueAsLong()));
                } else {
                    // value already set - increment by 1
                    kvpList.replace(ccv.getName(), Long.toString(Long.parseLong(kvpValue) + 1));
                }
            }
        }
    }


    /*
     *
     */
    private void getData(Connection connection, LocalDataFile batchFile, ConfigurationConnector configurationConnector, HashMap<String, String> kvpList) throws Exception {
        ArrayList<String> columnNameList = new ArrayList<String>();
        StringBuffer sb = null;
        ResultSetMetaData rsmd = null;
        Statement stmt = connection.createStatement();
        String adjustedSQL = adjustSQLStatement(configurationConnector.getSqlStatement(), kvpList);
        LOG.info("   Executing sql:" + adjustedSQL);
        ResultSet rs = stmt.executeQuery(adjustedSQL);

        LOG.info("   Saving content to: " + batchFile.getLocalPathFile());

        File temporaryDownloadFile = new File(batchFile.getLocalPathFile() + ".download");

        if (temporaryDownloadFile.exists())
            if (!temporaryDownloadFile.delete())
                throw new IOException("Could not delete existing temporary download file " + temporaryDownloadFile);

        // Open output file
        dataLogFileBuffer = new BufferedWriter(new FileWriter(temporaryDownloadFile));

        // Write headers
        rsmd = rs.getMetaData();

        sb = new StringBuffer();
        for (int a = 1; a <= rsmd.getColumnCount(); a++) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(rsmd.getColumnName(a));
        }
        dataLogFileBuffer.write(sb.toString());
        dataLogFileBuffer.newLine();

        // Write content
        while (rs.next()) {
            sb = new StringBuffer();
            for (int a = 1; a <= rsmd.getColumnCount(); a++) {
                String sqlField = rs.getString(a);
                if (sqlField == null) {
                    if (configurationConnector.isNullValueAsString()) {
                        sqlField = "";
                    } else {
                        sqlField = "~~NULL~~";
                    }
                }
                if (sb.length() > 0) {
                    sb.append(",");
                }
                if (sqlField.indexOf(",") >= 0) {
                    sb.append("\"");
                    sb.append(sqlField);
                    sb.append("\"");
                } else {
                    sb.append(sqlField);
                }
                // Ensure all values from last row is saved
                kvpList.put(rsmd.getColumnName(a), sqlField);
            }

            dataLogFileBuffer.write(sb.toString());
            dataLogFileBuffer.newLine();

        }

        rs.close();
        stmt.close();
        if (dataLogFileBuffer != null) {
            dataLogFileBuffer.flush();
            dataLogFileBuffer.close();
            dataLogFileBuffer = null;
        }

        File destination = new File(batchFile.getLocalPathFile());
        if (destination.exists())
            destination.delete();

        if (!temporaryDownloadFile.renameTo(destination))
            throw new IOException("Could not temporary download file to " + batchFile.getLocalPathFile());
    }

    /*
     *
     */
    private String adjustSQLStatement(String sqlstatement, HashMap<String, String> kvpList) {
        String ret = sqlstatement;
        Iterator<String> it = kvpList.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            ret = ret.replaceAll("\\Q${" + key + "}\\E", kvpList.get(key));
        }
        return ret;
    }

    /*
     *
     */
    private void createBatchDirectory(LocalDataFile batchFile) throws IOException {
        File localPath = new File(batchFile.getLocalPathBatch());
        if (!localPath.exists())
            if (!localPath.mkdirs())
                throw new IOException("Could not create path " + localPath);
    }

    /*
     *
     */
    private boolean validateBatches() {
        try {
            LOG.trace(" Getting batches ready for validation");
            List<Batch> incompleteBatches = db.getIncompleteBatches(configuration.getConfigurationId(), configurationBatch.getBatchname());
            LOG.trace(" There are {} batches ready for validation", Integer.toString(incompleteBatches.size()));

            if (incompleteBatches.size() > 0) {
                Batch lastCompleteBatch = db.getLastCompleteBatch(configuration.getConfigurationId(), configurationBatch.getBatchname());

                validateBatches(incompleteBatches, lastCompleteBatch);

                org.endeavourhealth.jdbcreader.utilities.SlackNotifier slackNotifier = new org.endeavourhealth.jdbcreader.utilities.SlackNotifier(configuration);
                slackNotifier.notifyCompleteBatches(configuration, configurationBatch, incompleteBatches);
            }

            return true;
        } catch (Exception e) {
            LOG.error("Error occurred during validation", e);
        }
        return false;
    }

    /*
     *
     */
    private void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch) throws JDBCValidationException {
        String batchIdentifiers = StringUtils.join(incompleteBatches
                .stream()
                .map(t -> t.getBatchIdentifier())
                .collect(Collectors.toList()), ", ");

        LOG.trace(" Validating batches: " + batchIdentifiers);

        BatchValidator batchValidator = ImplementationActivator.createSftpBatchValidator(configurationBatch.getInterfaceTypeName());
        batchValidator.validateBatches(incompleteBatches, lastCompleteBatch, configuration, db);

        LOG.trace(" Completed batch validation");
    }


    /*
     *
     */
    private void notifyEds() throws Exception {

        List<Batch> unnotifiedBatches = db.getUnnotifiedBatches(configuration.getConfigurationId(), configurationBatch.getBatchname());
        LOG.debug("There are {} complete batches for notification", unnotifiedBatches.size());

        if (unnotifiedBatches.isEmpty()) {
            return;
        }

        if (configuration.isUseKeycloak()) {
            LOG.trace("Initialising keycloak at: {}", configuration.getKeycloakTokenUri());

            KeycloakClient.init(configuration.getKeycloakTokenUri(),
                    configuration.getKeycloakRealm(),
                    configuration.getKeycloakUsername(),
                    configuration.getKeycloakPassword(),
                    configuration.getKeycloakClientId());

            try {
                Header response = KeycloakClient.instance().getAuthorizationHeader();

                LOG.trace("Keycloak authorization header is {}: {}", response.getName(), response.getValue());
            } catch (IOException e) {
                throw new JDBCReaderException("Error initialising keycloak", e);
            }
        }
        else {
            LOG.trace("Keycloak is not enabled");
        }

        //then attempt to notify EDS for each organisation
        int countSuccess = 0;
        int countFail = 0;

        for (Batch batch: unnotifiedBatches) {
            try {
                LOG.trace("Notifying EDS for batch : {}", batch.getBatchId());
                notify(batch);
                countSuccess ++;
                db.setBatchAsNotified(batch);
            } catch (Exception e) {
                countFail ++;
                LOG.error("Error occurred notifying EDS", e);
            }
        }

        LOG.info("Notified EDS successfully {} times and failed {}", countSuccess, countFail);
    }

    /*
     *
     */
    private void notify(Batch unnotifiedBatch) throws JDBCReaderException, IOException {
        //Prvious->NotificationCreator notificationCreator = ImplementationActivator.createSftpNotificationCreator(configurationBatch.getInterfaceTypeName());
        //Prvious->String messagePayload = notificationCreator.createNotificationMessage(configuration, unnotifiedBatch);
        String relativePath = unnotifiedBatch.getLocalPath().substring(configuration.getLocalRootPathPrefix().length());
        String fullPath = configuration.getLocalRootPathPrefix() + relativePath;
        List<String> files = new ArrayList<>();
        for (File f: new File(fullPath).listFiles()) {
            files.add(FilenameUtils.concat(relativePath, f.getName()));
        }
        String messagePayload = StringUtils.join(files, System.lineSeparator());

        UUID messageId = UUID.randomUUID();
        String organisationId = configurationBatch.getOrganisationId();
        String softwareContentType = configuration.getSoftwareContentType();
        String softwareVersion = configuration.getSoftwareVersion();
        String outboundMessage = EdsSender.buildEnvelope(messageId, organisationId, softwareContentType, softwareVersion, messagePayload);

        try {
            String edsUrl = configuration.getEdsUrl();
            boolean useKeycloak = configuration.isUseKeycloak();

            EdsSenderResponse edsSenderResponse = EdsSender.notifyEds(edsUrl, useKeycloak, outboundMessage);

            NotificationMessage notificationMessage = new NotificationMessage();
            notificationMessage.setBatchId(unnotifiedBatch.getBatchId());
            notificationMessage.setConfigurationId(configuration.getConfigurationId());
            notificationMessage.setMessageUuid(messageId.toString());
            notificationMessage.setOutbound(outboundMessage);
            notificationMessage.setInbound(edsSenderResponse.getStatusLine() + "\r\n" + edsSenderResponse.getResponseBody());
            notificationMessage.setSuccess(true);
            notificationMessage.setNotificationTimestamp(new Date());
            notificationMessage.setErrorText(null);
            db.addBatchNotification(notificationMessage);
            LOG.trace("Success NotificationMessage saved " + notificationMessage.getNotificationMessageId());

            // Set batch as notified (sent to EDS)
            db.setBatchAsComplete(unnotifiedBatch);

            //notify to Slack to say any previous error is now cleared, so we don't have to keep monitoring files
            if (shouldSendSlackOk(unnotifiedBatch.getBatchId())) {
                sendSlackOk(unnotifiedBatch.getBatchId(), organisationId);
            }

        } catch (Exception e) {
            String inboundMessage = null;

            if (e instanceof EdsSenderHttpErrorResponseException) {
                EdsSenderResponse edsSenderResponse = ((EdsSenderHttpErrorResponseException)e).getEdsSenderResponse();
                inboundMessage = edsSenderResponse.getStatusLine() + "\r\n" + edsSenderResponse.getResponseBody();
            }

            NotificationMessage notificationMessage = new NotificationMessage();
            notificationMessage.setBatchId(unnotifiedBatch.getBatchId());
            notificationMessage.setConfigurationId(configuration.getConfigurationId());
            notificationMessage.setMessageUuid(messageId.toString());
            notificationMessage.setOutbound(outboundMessage);
            notificationMessage.setInbound(inboundMessage);
            notificationMessage.setSuccess(false);
            notificationMessage.setNotificationTimestamp(new Date());
            notificationMessage.setErrorText(getExceptionNameAndMessage(e));
            try {
                db.addBatchNotification(notificationMessage);
                LOG.trace("Error NotificationMessage saved " + notificationMessage.getNotificationMessageId());
            } catch (Exception e1) {
                e1.printStackTrace();
            }

            //notify to Slack, so we don't have to keep monitoring files
            if (shouldSendSlackAlert(unnotifiedBatch.getBatchId(), inboundMessage)) {
                sendSlackAlert(unnotifiedBatch.getBatchId(), organisationId, inboundMessage);
            }

            throw new JDBCReaderException("Error notifying EDS for batch  " + unnotifiedBatch.getBatchId(), e);
        }
    }


    private static String getExceptionNameAndMessage(Throwable e) {
        String result = "[" + e.getClass().getName() + "] " + e.getMessage();

        if (e.getCause() != null)
            result += " | " + getExceptionNameAndMessage(e.getCause());

        return result;
    }

    /*
    private LocalDateTime calculateNextRunTime(LocalDateTime thisRunStartTime) {
        Validate.notNull(thisRunStartTime);
        return thisRunStartTime.plusSeconds(configurationBatch.getPollFrequencySeconds());
    }
    */


    private void sendSlackAlert(Long batchId, String organisationId, String errorMessage) {

        String message = "Exception notifying Messaging API for Organisation " + organisationId + " and Batch:" + batchId + "\r\n" + errorMessage;

        org.endeavourhealth.jdbcreader.utilities.SlackNotifier slackNotifier = new org.endeavourhealth.jdbcreader.utilities.SlackNotifier(configuration);
        slackNotifier.postMessage(message);

        //add to the map so we don't send the same message again in a few minutes
        notificationErrorrs.put(batchId, errorMessage);
    }

    /*
     *
     */
    private boolean shouldSendSlackAlert(Long batchId, String errorMessage) {

        if (!notificationErrorrs.containsKey(batchId)) {
            return true;
        }

        //don't keep sending the alert for the same error message
        String previousError = notificationErrorrs.get(batchId);
        if (previousError == null && errorMessage == null) {
            return false;
        }

        if (previousError != null
                && errorMessage != null
                && previousError.equals(errorMessage)) {
            return false;
        }

        return true;
    }

    private boolean shouldSendSlackOk(Long batchId) {
        return notificationErrorrs.containsKey(batchId);
    }

    private void sendSlackOk(Long batchId, String organisationId) {

        String message = "Previous error notifying Messaging API for Organisation " + organisationId + " and Batch  " + batchId + " is now cleared";

        org.endeavourhealth.jdbcreader.utilities.SlackNotifier slackNotifier = new SlackNotifier(configuration);
        slackNotifier.postMessage(message);

        //remove from the map, so we know we're in a good state now
        notificationErrorrs.remove(batchId);
    }


}
