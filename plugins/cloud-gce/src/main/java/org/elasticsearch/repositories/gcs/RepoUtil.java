package org.elasticsearch.repositories.gcs;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.unit.TimeValue;

public final class RepoUtil {

    private RepoUtil() {
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    public static <T> T doPrivileged(PrivilegedExceptionAction<T> operation) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }

    /**
     * Executes a {@link PrivilegedExceptionAction} with privileges enabled.
     */
    public static void doPrivilegedVoid(PrivilegedExceptionAction<Void> operation) throws IOException {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            AccessController.doPrivileged(operation);
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
    }

    /**
     * Converts timeout values from the settings to a timeout value for the Google
     * Cloud SDK
     **/
    public static Integer toTimeout(final TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if (timeout == null || TimeValue.timeValueMillis(0).equals(timeout)) {
            // negative value means using the default value
            return -1;
        }
        // -1 means infinite timeout
        if (TimeValue.timeValueMillis(-1).equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return (int) timeout.getMillis();
    }
}
