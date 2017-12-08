package org.endeavourhealth.jdbcreader.implementations;

import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;

public abstract class SlackNotifier {
    public abstract String getCompleteBatchMessageSuffix(Batch completeBatch);
}
