package org.endeavourhealth.jdbcreader.implementations;

import org.endeavourhealth.jdbcreader.Configuration;
import org.endeavourhealth.jdbcreader.implementations.homerton.*;

public class ImplementationActivator {
    // do this properly - instatiate dynamically based on configuration against interface type

    public static BatchValidator createSftpBatchValidator(String interfaceTypeName) {
        return new HomertonSftpBatchValidator();
    }

    public static SlackNotifier createSftpSlackNotifier(String interfaceTypeName) {
        return new HomertonSlackNotifier();
    }

}
