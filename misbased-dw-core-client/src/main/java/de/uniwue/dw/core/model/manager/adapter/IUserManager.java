package de.uniwue.dw.core.model.manager.adapter;

import java.sql.SQLException;

import de.uniwue.dw.core.client.authentication.User;
import de.uniwue.dw.core.client.authentication.UserSettings;

public interface IUserManager {

  void saveUserSettings(User user, UserSettings settings) throws SQLException;

}
