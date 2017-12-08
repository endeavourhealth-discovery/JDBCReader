package org.endeavourhealth.jdbcreader.implementations.homerton;

import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;
import org.endeavourhealth.jdbcreader.implementations.SlackNotifier;

import java.text.MessageFormat;

public class HomertonSlackNotifier extends SlackNotifier {

    public String getCompleteBatchMessageSuffix(Batch completeBatch) {
        return MessageFormat.format(" with id of {0}, total file count {1}", completeBatch.getBatchIdentifier(), completeBatch.getBatchFiles().size());
    }
}
