package org.endeavourhealth.jdbcreader.utilities;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;
import org.endeavourhealth.jdbcreader.Configuration;
import org.endeavourhealth.jdbcreader.ConfigurationBatch;
import org.endeavourhealth.jdbcreader.implementations.ImplementationActivator;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SlackNotifier {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SlackNotifier.class);

    private Configuration configuration;
    //private DbInstanceSlack slackConfiguration;

    public SlackNotifier(Configuration configuration) {
        Validate.notNull(configuration, "configuration");
        //Validate.notNull(configuration.getInstanceConfiguration().getSlackConfiguration(), "configuration.getDbGlobalConfiguration().getSlackConfiguration()");

        this.configuration = configuration;
        //this.slackConfiguration = configuration.getInstanceConfiguration().getSlackConfiguration();
    }

    /*public void notifyStartup() {
        String message = Main.PROGRAM_DISPLAY_NAME + " started "
                + "(" + this.configuration.getInstanceName() + " instance, reading extracts "
                + this.configuration.getConfigurationIdsForDisplay() + ")";

        postMessage(message);
    }

    public void notifyShutdown() {
        String message = Main.PROGRAM_DISPLAY_NAME + " stopped (" + this.configuration.getInstanceName() + " instance)";
        postMessage(message);
    }*/

    public void notifyCompleteBatches(Configuration configuration, ConfigurationBatch configurationBatch, List<Batch> batches) {
        for (Batch batch : batches)
            notifyCompleteBatch(configuration, configurationBatch, batch);
    }

    private void notifyCompleteBatch(Configuration configuration, ConfigurationBatch configurationBatch, Batch batch) {

        String configurationId = configuration.getConfigurationId();
        String friendlyName = configuration.getConfigurationFriendlyName();

        String message = friendlyName + " extract (" + configurationId + ") received";

        org.endeavourhealth.jdbcreader.implementations.SlackNotifier slackNotifier = ImplementationActivator.createSftpSlackNotifier(configurationBatch.getInterfaceTypeName());
        message += slackNotifier.getCompleteBatchMessageSuffix(batch);

        postMessage(message);
    }

    public void postMessage(String slackMessage) {
        try {
            //changing over to use common slack library rather than different for each
            LOG.info("Posting message to slack: '" + slackMessage + "'");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.JDBCReaderAlertsHomerton, slackMessage);

            /*if (!slackConfiguration.isEnabled())
                return;

            SlackApi slackApi = new SlackApi(slackConfiguration.getSlackUrl());
            slackApi.call(new SlackMessage(slackMessage));*/

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }
}
