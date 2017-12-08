package org.endeavourhealth.jdbcreader.implementations;

import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;
import org.endeavourhealth.jdbcreader.Configuration;
import org.endeavourhealth.jdbcreader.DataLayer;
import org.endeavourhealth.jdbcreader.utilities.JDBCValidationException;

import java.util.List;

public abstract class BatchValidator {
    public abstract void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch, Configuration configuration, DataLayer db) throws JDBCValidationException;
}
