package org.endeavourhealth.jdbcreader;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.jdbcreader.JDBCReaderDalI;
import org.endeavourhealth.core.database.dal.jdbcreader.models.Batch;
import org.endeavourhealth.core.database.dal.jdbcreader.models.BatchFile;
import org.endeavourhealth.core.database.dal.jdbcreader.models.KeyValuePair;
import org.endeavourhealth.core.database.dal.jdbcreader.models.NotificationMessage;
import org.endeavourhealth.core.database.rdbms.jdbcreader.RdbmsJDBCReaderDal;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DataLayer {
    public static final String BATCH_SEQ_NO = "batchSequenceNumber";
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DataLayer.class);

    private DataSource dataSource;

    public DataLayer() {
    }

    public DataLayer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /*
    public List<Batch> getIncompleteBatches(String configurationId, String batchname) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        return database.getIncompleteBatches(configurationId, batchname);
    }


    public Batch getLastCompleteBatch(String configurationId, String batchname) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        Batch batch = database.getLastCompleteBatch(configurationId, batchname);
        return batch;
    }

    public Batch getLastBatch(String configurationId, String batchname) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        Batch batch = database.getLastBatch(configurationId, batchname);
        return batch;
    }

    public List<Batch> getUnnotifiedBatches(String configurationId, String batchname) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        return database.getUnnotifiedBatches(configurationId, batchname);
    }

    public void setBatchAsComplete(Batch batch) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        database.setBatchAsComplete(batch);
    }

    public void setBatchFileAsDownloaded(BatchFile batchFile) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        database.setBatchFileAsDownloaded(batchFile);
    }

    public void setBatchAsNotified(Batch batch) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        database.setBatchAsNotified(batch);
    }

    public void addBatch(Batch batch) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        database.insertBatch(batch);
    }

    public void addBatchFile(BatchFile batchFile) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        database.insertBatchFile(batchFile);
    }

    public void addBatchNotification(NotificationMessage notificationMessage) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        database.insertNotificationMessage(notificationMessage);
    }

    public BatchFile getBatchFile(Long batchId, String fileName) throws Exception {
        RdbmsJDBCReaderDal database = DalProvider.factoryJDBCReaderDal();
        return database.getBatchFile(batchId, fileName);
    }*/

    /*
       *
    */
    public KeyValuePair getKeyValuePair(KeyValuePair kvp) throws Exception {
        JDBCReaderDalI database = DalProvider.factoryJDBCReaderDal();
        return database.getKeyValuePair(kvp);
    }

    public KeyValuePair getKeyValuePair(String batchName, String connectionName, String key) throws Exception {
        KeyValuePair kvp = new KeyValuePair();
        kvp.setBatchName(batchName);
        kvp.setConnectionName(connectionName);
        kvp.setKeyValue(key);
        JDBCReaderDalI database = DalProvider.factoryJDBCReaderDal();
        return getKeyValuePair(kvp);
    }

    /*
     *
     */
    public List<KeyValuePair> getKeyValuePairs(String batchname, String connectionName) throws Exception {
        JDBCReaderDalI database = DalProvider.factoryJDBCReaderDal();
        return database.getKeyValuePairs(batchname, connectionName);
    }


    /*
     *
     */
    public void addKeyValuePair(String batchName, String connectionName, String key, String data) throws Exception {
        KeyValuePair kvp = new KeyValuePair();
        kvp.setBatchName(batchName);
        kvp.setConnectionName(connectionName);
        kvp.setKeyValue(key);
        kvp.setDataValue(data);
        addKeyValuePair(kvp);
    }

    public void addKeyValuePair(KeyValuePair kvp) throws Exception {
        JDBCReaderDalI database = DalProvider.factoryJDBCReaderDal();
        database.insertUpdateKeyValuePair(kvp);
    }

    public void insertUpdateKeyValuePair(String batchName, String connectionName, HashMap<String, String> kvpList) throws Exception {
        JDBCReaderDalI database = DalProvider.factoryJDBCReaderDal();
        Iterator<String> it = kvpList.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            KeyValuePair kvp = new KeyValuePair();
            kvp.setBatchName(batchName);
            kvp.setConnectionName(connectionName);
            kvp.setKeyValue(key);
            kvp.setDataValue(kvpList.get(key));
            addKeyValuePair(kvp);
        }
    }

}
