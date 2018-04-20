package org.endeavourhealth.jdbcreader;

import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.utility.SlackHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	public static final String PROGRAM_DISPLAY_NAME = "JDBC Reader";
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;
    private static JdbcReaderTaskScheduler jdbcReaderTaskScheduler;

	public static void main(String[] args) {
		try {
            LOG.info("Initialising config manager");
            ConfigManager.Initialize("jdbcreader");

            LOG.info("Fetching jdbcreader configuration");
            configuration = new Configuration(ConfigManager.getConfigurationAsJson("jdbcreaderconfig"));

            try {
                String startupText = "Instance " + configuration.getInstanceName() + " on host " + configuration.getMachineName() + " started";

                LOG.info("--------------------------------------------------");
                LOG.info(PROGRAM_DISPLAY_NAME);
                LOG.info("--------------------------------------------------");

                LOG.info(startupText);

                if (configuration.notifyStartStopSlack()) {
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.JDBCReaderAlertsHomerton, startupText);
                }

                Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));

                jdbcReaderTaskScheduler = new JdbcReaderTaskScheduler(configuration);
                jdbcReaderTaskScheduler.start();

            } finally {
            }
        } catch (ConfigManagerException cme) {
            printToErrorConsole("Fatal exception occurred initializing ConfigManager", cme);
            LOG.error("Fatal exception occurred initializing ConfigManager", cme);
            System.exit(-2);
        }
        catch (Exception e) {
            LOG.error("Fatal exception occurred", e);
            SlackHelper.sendSlackMessage(SlackHelper.Channel.JDBCReaderAlertsHomerton,"Fatal error in JDBCReader", e);
            System.exit(-1);
        }

        if (configuration.notifyStartStopSlack()) {
            SlackHelper.sendSlackMessage(SlackHelper.Channel.JDBCReaderAlertsHomerton, "JDBCReader stopped");
        }
    }

    private static void shutdown() {
        try {
            LOG.info("Shutting down...");

            /*if (slackNotifier != null)
                slackNotifier.notifyShutdown();*/

            if (jdbcReaderTaskScheduler != null)
                jdbcReaderTaskScheduler.stop();

        } catch (Exception e) {
            printToErrorConsole("Exception occurred during shutdown", e);
            LOG.error("Exception occurred during shutdown", e);
        }
    }

    private static void printToErrorConsole(String message, Exception e) {
        System.err.println(message + " [" + e.getClass().getName() + "] " + e.getMessage());
    }
}

