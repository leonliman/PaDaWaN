package de.uniwue.dw.core.client.authentication;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.manager.adapter.IUserSettingsAdapter;
import de.uniwue.misc.util.ConfigException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.AuthenticationException;
import javax.security.auth.login.AccountException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ServiceLoader;

public class DWAuthenticator {

    private static final Logger logger = LogManager.getLogger(DWAuthenticator.class);

    private static final IUserSettingsAdapter settingsAdapter = initUserSettingsAdapter();

    /**
     * @param username
     * @param password
     * @return a user object if authentication is succesfull
     * @throws AuthenticationException if no authentication services are reachable
     * @throws AccountException        if username or password is wrong
     */
    public static User getUser(String username, String password)
            throws AuthenticationException, AccountException {
        User user = null;
        boolean online = false;
        ServiceLoader<AuthenticationService> loader = ServiceLoader.load(AuthenticationService.class, AuthenticationService.class.getClassLoader());
        for (AuthenticationService auth : loader) {
            try {
                if (auth.isOnline()) {
                    online = true;
                    user = auth.authenticate(username, password);
                    logger.info("user authenticated (" + user.toString() + ")");
                }
            } catch (ConfigException e) {
                online = false;
            } catch (AccountException e) {
                // no match by this service. try next
            }
        }
        if (!online) {
            throw new AuthenticationException("no authentication services online");
        }
        if (user == null) {
            throw new AccountException("wrong username or password");
        }
        loadUserSettings(user);
        return user;
    }

    private static IUserSettingsAdapter initUserSettingsAdapter() {
        try {
            return DwClientConfiguration.getInstance().getClientAdapterFactory().createUserSettingsAdapter();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("could not create SQLUserSettingsAdapter ", e);
        }
        return null;
    }

    private static void loadUserSettings(User user) {
        UserSettings settings = null;
        if (settingsAdapter != null) {
            try {
                settings = settingsAdapter.getUserSettings(user);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("Settings for user " + user.getUsername()
                    + " could not be load. Setting default settings.");
        }
        if (settings == null)
            settings = UserSettings.createDefaultUserSettings(user.isAdmin());
        user.setSettings(settings);
    }

    /**
     * @param username
     * @return a String[] with infos for the given username
     * @throws AccountException        if username is unknown
     * @throws AuthenticationException if no authentications services are online
     */
    public static String[] getInfosForUsername(String username)
            throws AccountException, AuthenticationException {
        String[] userData = null;
        ServiceLoader<AuthenticationService> loader = ServiceLoader.load(AuthenticationService.class);
        boolean online = false;
        for (AuthenticationService auth : loader) {
            if (auth.isOnline()) {
                online = true;
                try {
                    userData = auth.requestUserData(username);
                } catch (AccountException e) {
                    // nothing to do here
                } catch (ConfigException e) {
                }
            }
        }
        if (!online) {
            throw new AuthenticationException("no authentication services online");
        }
        if (userData == null) {
            throw new AccountException("username unknown or all systems down");
        }
        return userData;
    }

    // you can use this to quickly resolve a username manually
    public static void main(String[] args) throws AuthenticationException, IOException {
        try {
            String[] userData = getInfosForUsername("");
            System.out.println("Vorname: " + userData[0]);
            System.out.println("Nachname: " + userData[1]);
            System.out.println("Email: " + userData[2]);
        } catch (AccountException e) {
            e.printStackTrace();
        }
    }
}
