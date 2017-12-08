package org.endeavourhealth.jdbcreader;

import org.endeavourhealth.jdbcreader.utilities.JDBCValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class JdbcReaderTaskInfo {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcReaderTaskInfo.class);
    private JdbcReaderTask jdbcReaderTask;
    private ConfigurationBatch configurationBatch;
    private LocalDateTime nextScheduledDate;
    private String taskName;

    public JdbcReaderTaskInfo(JdbcReaderTask jdbcReaderTask, ConfigurationBatch configurationBatch) {
        this.jdbcReaderTask = jdbcReaderTask;
        this.configurationBatch = configurationBatch;
        if (configurationBatch.getPollStart() != null && configurationBatch.getPollStart().length() > 0) {
            String hours = configurationBatch.getPollStart().split(":")[0];
            String minutes = configurationBatch.getPollStart().split(":")[1];
            //LOG.trace("Setting start time to " + hours + " and " + minutes + " minutes");
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime ldt = LocalDateTime.of(now.getYear(), now.getMonth(), now.getDayOfMonth(), 0, 0);
            ldt = ldt.plusHours(Integer.parseInt(hours));
            ldt = ldt.plusMinutes(Integer.parseInt(minutes));
            if (ldt.isBefore(LocalDateTime.now())) {
                // We have already passed start time - start tomorrow instead
                ldt = ldt.plusDays(1);
            }
            this.nextScheduledDate = ldt;
            LOG.trace("JdbcReaderTask " + configurationBatch.getBatchname() + " first scheduled for " + ldt.format(JdbcReaderTaskScheduler.DATE_DISPLAY_FORMAT));
        } else {
            this.nextScheduledDate = LocalDateTime.now();
        }
        this.taskName = configurationBatch.getBatchname();
    }

    public void runTask() throws InterruptedException, JDBCValidationException {
        try {
            Thread thread = new Thread(jdbcReaderTask, getTaskName());
            thread.start();
            thread.join();
        } finally {
            this.incrementScheduledDate();
        }
    }

    public LocalDateTime getNextScheduledDate() {
        return this.nextScheduledDate;
    }

    public void incrementScheduledDate() throws JDBCValidationException {
        String timeUnits = configurationBatch.getPollFrequency().substring(configurationBatch.getPollFrequency().length() - 1);
        String time = configurationBatch.getPollFrequency().substring(0, configurationBatch.getPollFrequency().length() - 1);
        //LOG.trace("Increment schedule by " + time +  " units=" + timeUnits);
        if (timeUnits.toUpperCase().compareTo("S") == 0) {
            this.nextScheduledDate = LocalDateTime.now().plusSeconds(Integer.parseInt(time));
        } else if (timeUnits.toUpperCase().compareTo("M") == 0) {
            this.nextScheduledDate = LocalDateTime.now().plusMinutes(Integer.parseInt(time));
        } else if (timeUnits.toUpperCase().compareTo("H") == 0) {
            this.nextScheduledDate = LocalDateTime.now().plusHours(Integer.parseInt(time));
        } else {
            throw new JDBCValidationException("Exception occurred when calculating next schedule using " + configurationBatch.getPollFrequency());
        }
    }

    public String getTaskName() {
        return this.taskName;
    }
}
