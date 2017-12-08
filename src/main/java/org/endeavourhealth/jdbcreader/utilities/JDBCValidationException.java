package org.endeavourhealth.jdbcreader.utilities;

public class JDBCValidationException extends JDBCReaderException {
    static final long serialVersionUID = 0L;

    public JDBCValidationException() {
        super();
    }
    public JDBCValidationException(String message) {
        super(message);
    }
    public JDBCValidationException(String message, Throwable cause) {
        super(message, cause);
    }
    public JDBCValidationException(Throwable cause) {
        super(cause);
    }
}
