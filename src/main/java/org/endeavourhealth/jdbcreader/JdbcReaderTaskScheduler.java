package org.endeavourhealth.jdbcreader;

import org.endeavourhealth.jdbcreader.utilities.JDBCValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class JdbcReaderTaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcReaderTaskScheduler.class);
    static final DateTimeFormatter DATE_DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MMM-dd HH:mm:ss");
    private static final int THREAD_SLEEP_SECONDS = 60;

    private Configuration configuration;
    private List<JdbcReaderTaskInfo> tasks;

    public JdbcReaderTaskScheduler(Configuration configuration) {
        this.configuration = configuration;
    }

    public void start() throws InterruptedException, JDBCValidationException {

        LOG.info("Starting JdbcReaderTaskScheduler");

        this.tasks = createTasks(configuration);

        while (true) {

            for (JdbcReaderTaskInfo task : tasks) {

                if (task.getNextScheduledDate().isBefore(LocalDateTime.now())) {

                    LOG.info("Starting JdbcReaderTask " + task.getTaskName());

                    LOG.trace("--------------------------------------------------");
                    task.runTask();
                    LOG.trace("--------------------------------------------------");

                    LOG.info("Completed JdbcReaderTask " + task.getTaskName());

                    LOG.trace("JdbcReaderTask " + task.getTaskName() + " next scheduled for " + task.getNextScheduledDate().format(DATE_DISPLAY_FORMAT));
                }
            }

            Thread.sleep(THREAD_SLEEP_SECONDS);
        }
    }

    public void stop() {
    }

    private static List<JdbcReaderTaskInfo> createTasks(Configuration configuration) {

        List<JdbcReaderTaskInfo> tasks = new ArrayList<>();

        LOG.info("Number of batches:" + configuration.getBatchConfigurations().size());

        for (ConfigurationBatch batchConfiguration : configuration.getBatchConfigurations()) {
            if (batchConfiguration.isActive()) {
                LOG.info("Creating JDBCReaderTask for batch configuration " + batchConfiguration.getBatchname());

                JdbcReaderTask jdbcReaderTask = new JdbcReaderTask(configuration, batchConfiguration.getBatchname());

                tasks.add(new JdbcReaderTaskInfo(jdbcReaderTask, batchConfiguration));
            } else {
                LOG.info("JDBCReaderTask batch is inactive " + batchConfiguration.getBatchname());
            }
        }

        return tasks;
    }
}
