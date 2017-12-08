package org.endeavourhealth.jdbcreader.utilities;

public class JDBCReaderException extends Exception {
    static final long serialVersionUID = 0L;

    public JDBCReaderException() {
        super();
    }
    public JDBCReaderException(String message) {
        super(message);
    }
    public JDBCReaderException(String message, Throwable cause) {
        super(message, cause);
    }
    public JDBCReaderException(Throwable cause) {
        super(cause);
    }
}
