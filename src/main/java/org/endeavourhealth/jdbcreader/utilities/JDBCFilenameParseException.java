package org.endeavourhealth.jdbcreader.utilities;

public class JDBCFilenameParseException extends JDBCReaderException {
    static final long serialVersionUID = 0L;

    public JDBCFilenameParseException() {
        super();
    }
    public JDBCFilenameParseException(String message) {
        super(message);
    }
    public JDBCFilenameParseException(String message, Throwable cause) {
        super(message, cause);
    }
    public JDBCFilenameParseException(Throwable cause) {
        super(cause);
    }
}
