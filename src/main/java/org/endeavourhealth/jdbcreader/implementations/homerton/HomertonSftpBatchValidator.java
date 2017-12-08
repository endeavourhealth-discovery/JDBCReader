package org.endeavourhealth.jdbcreader.implementations.homerton;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;
import org.endeavourhealth.jdbcreader.Configuration;
import org.endeavourhealth.jdbcreader.DataLayer;
import org.endeavourhealth.jdbcreader.implementations.BatchValidator;
import org.endeavourhealth.jdbcreader.utilities.JDBCValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HomertonSftpBatchValidator extends BatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonSftpBatchValidator.class);

    @Override
    public void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch, Configuration configuration, DataLayer db) throws JDBCValidationException {
        Validate.notNull(incompleteBatches, "incompleteBatches is null");
        Validate.notNull(configuration, "dbConfiguration is null");
        Validate.notNull(configuration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(configuration.getInterfaceFileTypes(), "No interface file types configured");
    }


}
