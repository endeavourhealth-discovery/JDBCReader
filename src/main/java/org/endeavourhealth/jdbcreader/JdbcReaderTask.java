package org.endeavourhealth.jdbcreader;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.jdbcreader.models.KeyValuePair;
import org.endeavourhealth.jdbcreader.utilities.JDBCReaderException;
import org.endeavourhealth.jdbcreader.utilities.SlackNotifier;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class JdbcReaderTask implements Runnable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(JdbcReaderTask.class);
    private Map<Long, String> notificationErrorrs = new HashMap<>();

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
            //Batch currentbatch = getOrCreateBatch();
            //LOG.trace("Current BatchId:" + currentbatch.getBatchId());

            LOG.trace(">>>Retrieving data");
            if (!downloadFiles()) {
                throw new JDBCReaderException("Exception occurred downloading and processing files - halting to prevent incorrect ordering.");
            }

            //LOG.trace(">>>Validating batches");
            //if (!validateBatches()) {
              //  throw new JDBCReaderException("Exception occurred validating batches - halting to prevent incorrect ordering of batches.");
            //}

            //LOG.trace(">>>Notifying EDS");
            //notifyEds();

            LOG.trace(">>>Completed JDBCReader run");

        } catch (Exception e) {
            LOG.error(">>>Fatal exception in JDBCTask run, terminating this run", e);
        }
    }

    /*
     * Get last Batch record for this batch name
     */
    /*
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
    }*/

    /*
     *
     */
    private void initialise() throws Exception {
        this.configurationBatch = configuration.getBatchConfiguration(configurationId);

        // Temporary destination must be local - cannot be S3
        if (StringUtils.isNotEmpty(this.configuration.getTempPathPrefix())) {
            File rootPath = new File(this.configuration.getTempPathPrefix());
            if ((!rootPath.exists()) || (!rootPath.isDirectory()))
                throw new JDBCReaderException("Local temp path '" + rootPath + "' does not exist");
        }

        // Final destination can be local file or S3
        if (this.configuration.getDestinationPathPrefix() != null && StringUtils.isNotEmpty(this.configuration.getDestinationPathPrefix())) {
            if (FileHelper.directoryExists(this.configuration.getDestinationPathPrefix()) == false)
                throw new JDBCReaderException("Root path '" + this.configuration.getDestinationPathPrefix() + "' does not exist");
        }
    }

    /*
     *
     */
    private boolean downloadFiles() {
        HashMap<String, Connection> connectionList = new HashMap<String, Connection>();
        ArrayList<File> tempFiles = new ArrayList<File>();

        try {
            LOG.trace("Found {} connection(s) in batch {}", configurationBatch.getConnections().size(), configurationBatch.getBatchname());

            FileHelper.createDirectoryIfNotExists(this.configuration.getTempPathPrefix());

            if (this.configuration.getDestinationPathPrefix() != null && this.configuration.getDestinationPathPrefix().length() > 0) {
                FileHelper.createDirectoryIfNotExists(this.configuration.getDestinationPathPrefix());
            }

            // Loop through all connections for this batch
            for (ConfigurationConnector configurationConnector : configurationBatch.getConnections()) {
                if (configurationConnector.isActive()) {
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

                    //check if paramdate KVP is not in the future as this would throw out data extraction
                    if (isParamDateInFuture(kvpList)) {
                        LOG.trace("KVP entry <paramdate>: {} in future.....skipping connector", kvpList.get("paramdate"));
                        continue;
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
                        } catch (SQLException e) {
                            LOG.trace("SQLException - Auto load of driver not working");
                            LOG.trace("Loading driver (" + configurationConnector.getDriver() + ") .....");
                            Class.forName(configurationConnector.getDriver());
                            LOG.trace("......Driver loaded - trying again");
                            connection = DriverManager.getConnection(configurationConnector.getSqlURL());
                        }
                    }

                    File temporaryFile = getData(connection, this.configuration.getTempPathPrefix(), replaceVariablesWithValues(configurationConnector.getFilename(), kvpList), configurationConnector, kvpList);
                    tempFiles.add(temporaryFile);

                    LOG.trace("Saving " + kvpList.size() + " KVP entries");
                    db.insertUpdateKeyValuePair(configurationBatch.getBatchname(), configurationConnector.getConnectorName(), kvpList);
                } else {
                    LOG.trace("Connector is inactive " + configurationConnector.getConnectorName());
                }
            }

            // Move from temp to archive
            LOG.info("Completed processing {} files.", tempFiles.size());
            if (this.configuration.getDestinationPathPrefix() != null && this.configuration.getDestinationPathPrefix().length() > 0) {
                List<String> filesToMove = FileHelper.listFilesInSharedStorage(this.configuration.getTempPathPrefix());
                LOG.info("Moving {} file(s) from temp storage to archive {}", filesToMove.size(), this.configuration.getDestinationPathPrefix());
                for (String file : filesToMove) {
                    File f = new File(file);
                    //String newFileName = FilenameUtils.concat(this.configuration.getDestinationPathPrefix(), f.getName());
                    String newFileName = this.configuration.getDestinationPathPrefix() + File.separator + f.getName();
                    LOG.info("Moving file " + f.getAbsolutePath() + " to " + newFileName);
                    FileHelper.writeFileToSharedStorage(newFileName, f);
                    if (configurationBatch.removeTempFile()) {
                        if (!f.delete())
                            throw new IOException("Could not delete existing temporary download file " + f.getAbsolutePath());
                    }
                }
            }

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

    private boolean isParamDateInFuture(HashMap<String, String> kvpList) throws Exception {

        String kvpParamDate = kvpList.get("paramdate");
        if (!Strings.isNullOrEmpty(kvpParamDate)) {
            Date kvpDate = new SimpleDateFormat("yyyyMMdd").parse(kvpParamDate);
            Date today = new Date();
            return today.before(kvpDate);
        } else {
            //connector has no paramdate do just return as false
            return false;
        }
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
    private File getData(Connection connection, String tempFilebatchDir, String tempFileName, ConfigurationConnector configurationConnector, HashMap<String, String> kvpList) throws Exception {
        File temporaryDownloadFile = null;
        BufferedWriter temporaryDownloadFileBuffer = null;
        ZipOutputStream zos = null;
        ArrayList<String> columnNameList = new ArrayList<String>();
        StringBuffer sb = null;
        ResultSetMetaData rsmd = null;

        Statement stmt = connection.createStatement();
        String adjustedSQL = replaceVariablesWithValues(configurationConnector.getSqlStatement(), kvpList);
        LOG.info("   Executing sql:" + adjustedSQL);
        ResultSet rs = stmt.executeQuery(adjustedSQL);

        // Get headers
        rsmd = rs.getMetaData();

        sb = new StringBuffer();
        for (int a = 1; a <= rsmd.getColumnCount(); a++) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(rsmd.getColumnName(a));
        }
        String headerLine = sb.toString();

        if (configurationConnector.writeEmptyFileIfNothingFound()) {
            if (configurationBatch.zipDestinationFile()) {
                temporaryDownloadFile = new File(FileHelper.combinePaths(tempFilebatchDir, tempFileName)+ ".zip");
                if (temporaryDownloadFile.exists())
                    if (!temporaryDownloadFile.delete())
                        throw new IOException("Could not delete existing temporary download file " + temporaryDownloadFile);
                LOG.info("   Saving content to: " + temporaryDownloadFile);
                // Open output file
                FileOutputStream dest = new FileOutputStream(temporaryDownloadFile);
                zos = new ZipOutputStream(new BufferedOutputStream(dest));
                zos.putNextEntry(new ZipEntry(tempFileName + ".csv"));
                // Write header
                zos.write(headerLine.getBytes());
                zos.write("\r\n".getBytes());
            } else {
                temporaryDownloadFile = new File(FileHelper.combinePaths(tempFilebatchDir, tempFileName)+ ".csv");
                if (temporaryDownloadFile.exists())
                    if (!temporaryDownloadFile.delete())
                        throw new IOException("Could not delete existing temporary download file " + temporaryDownloadFile);
                LOG.info("   Saving content to: " + temporaryDownloadFile);
                // Open output file
                temporaryDownloadFileBuffer = new BufferedWriter(new FileWriter(temporaryDownloadFile));
                // Write header
                temporaryDownloadFileBuffer.write(headerLine);
                temporaryDownloadFileBuffer.newLine();
            }
        }

        // Write content
        while (rs.next()) {
            sb = new StringBuffer();
            for (int a = 1; a <= rsmd.getColumnCount(); a++) {
                String sqlField = rs.getString(a);
                if (sqlField == null) {
                    if (configurationConnector.isNullValueAsString()) {
                        sqlField = "";
                    } else {
                        sqlField = "NULL";
                    }
                }
                if (sb.length() > 0) {
                    sb.append(",");
                }

                sqlField = sqlField.replaceAll("\"","\\\"");

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

            if (temporaryDownloadFileBuffer == null && zos == null) {
                if (configurationBatch.zipDestinationFile()) {
                    temporaryDownloadFile = new File(FileHelper.combinePaths(tempFilebatchDir, tempFileName)+ ".zip");
                    if (temporaryDownloadFile.exists())
                        if (!temporaryDownloadFile.delete())
                            throw new IOException("Could not delete existing temporary download file " + temporaryDownloadFile);
                    LOG.info("   Saving content to: " + temporaryDownloadFile);
                    // Open output file
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    zos = new ZipOutputStream(baos);
                    zos.putNextEntry(new ZipEntry(tempFileName + ".csv"));
                    // Write header
                    zos.write(sb.toString().getBytes());
                    zos.write("\r\n".getBytes());
                } else {
                    temporaryDownloadFile = new File(FileHelper.combinePaths(tempFilebatchDir, tempFileName)+ ".csv");
                    if (temporaryDownloadFile.exists())
                        if (!temporaryDownloadFile.delete())
                            throw new IOException("Could not delete existing temporary download file " + temporaryDownloadFile);
                    LOG.info("   Saving content to: " + temporaryDownloadFile);
                    // Open output file
                    temporaryDownloadFileBuffer = new BufferedWriter(new FileWriter(temporaryDownloadFile));
                    // Write header
                    temporaryDownloadFileBuffer.write(sb.toString());
                    temporaryDownloadFileBuffer.newLine();
                }
            }

            if (configurationBatch.zipDestinationFile()) {
                zos.write(sb.toString().getBytes());
                zos.write("\r\n".getBytes());
            } else {
                temporaryDownloadFileBuffer.write(sb.toString());
                temporaryDownloadFileBuffer.newLine();
            }
        }

        rs.close();
        stmt.close();

        if (temporaryDownloadFileBuffer != null) {
            temporaryDownloadFileBuffer.flush();
            temporaryDownloadFileBuffer.close();
        }

        if (zos != null) {
            zos.flush();
            zos.close();
        }

        return temporaryDownloadFile;

    }

    /*
     *
     */
    private String replaceVariablesWithValues(String sqlstatement, HashMap<String, String> kvpList) {
        String ret = sqlstatement;
        Iterator<String> it = kvpList.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            ret = ret.replaceAll("\\Q${" + key + "}\\E", kvpList.get(key));
        }

        // Some additional ones
        String uuid = UUID.randomUUID().toString();
        ret = ret.replaceAll("\\Q${UUID}\\E", uuid);

        Date d = new Date();
        SimpleDateFormat sdfNow = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        SimpleDateFormat sdfYYYYMMDD = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdfYYYY_MM_DD = new SimpleDateFormat("yyyy-MM-dd");
        ret = ret.replaceAll("\\Q${NOW}\\E", sdfNow.format(d));
        ret = ret.replaceAll("\\Q${NOW-YYYYMMDD}\\E", sdfYYYYMMDD.format(d));
        ret = ret.replaceAll("\\Q${NOW-YYYY-MM-DD}\\E", sdfYYYY_MM_DD.format(d));

        return ret;
    }

    /*
     *
     */
    /*
    private void createBatchDirectories(LocalDataFile batchFile) throws IOException {
        if (FileHelper.createDirectory(batchFile.getLocalPath())) {
            if (!FileHelper.createDirectory(batchFile.getLocalPathBatch())) {
                throw new IOException("Could not create path " + batchFile.getLocalPathBatch());
            }
        } else {
            throw new IOException("Could not create path " + batchFile.getLocalPath());
        }

        if (FileHelper.createDirectory(batchFile.getTempPath())) {
            if (!FileHelper.createDirectory(batchFile.getTempPathBatch())) {
                throw new IOException("Could not create path " + batchFile.getTempPathBatch());
            }
        } else {
            throw new IOException("Could not create path " + batchFile.getTempPath());
        }
    }*/

    /*
     *
     */
    /*
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
    }*/

    /*
     *
     */
    /*
    private void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch) throws JDBCValidationException {
        String batchIdentifiers = StringUtils.join(incompleteBatches
                .stream()
                .map(t -> t.getBatchIdentifier())
                .collect(Collectors.toList()), ", ");

        LOG.trace(" Validating batches: " + batchIdentifiers);

        BatchValidator batchValidator = ImplementationActivator.createSftpBatchValidator(configurationBatch.getInterfaceTypeName());
        batchValidator.validateBatches(incompleteBatches, lastCompleteBatch, configuration, db);

        LOG.trace(" Completed batch validation");
    }*/


    /*
     *
     */
    /*
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
    }*/

    /*
     *
     */
    /*
    private void notify(Batch unnotifiedBatch) throws JDBCReaderException, IOException {
        //Prvious->NotificationCreator notificationCreator = ImplementationActivator.createSftpNotificationCreator(configurationBatch.getInterfaceTypeName());
        //Prvious->String messagePayload = notificationCreator.createNotificationMessage(configuration, unnotifiedBatch);
        LOG.trace("getLocalPath=" + unnotifiedBatch.getLocalPath());
        String relativePath = unnotifiedBatch.getLocalPath().substring(configuration.getLocalRootPathPrefix().length());
        LOG.trace("relativePath=" + relativePath);
        String fullPath = configuration.getLocalRootPathPrefix() + relativePath;
        LOG.trace("fullPath=" + fullPath);
        List<String> files = new ArrayList<>();
        for (File f: new File(fullPath).listFiles()) {
            files.add(FilenameUtils.concat(relativePath.substring(1), f.getName()));
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
