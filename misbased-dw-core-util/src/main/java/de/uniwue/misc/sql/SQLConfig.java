package de.uniwue.misc.sql;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import de.uniwue.misc.util.FileUtilsUniWue;

/**
 * The SQLConfig defines the coordinates to access a database. The information can be either given
 * as a JDBC url or via server, database, user, password. In either way the correct dbType has to be
 * set.
 * 
 * @author Georg Fette
 */
@XmlRootElement
public class SQLConfig implements ISqlConfigKeys {

  @XmlAttribute
  public String user = "";

  @XmlAttribute
  public String database = "";

  @XmlAttribute
  public String password = "";

  @XmlAttribute
  public String sqlServer = "";

  @XmlAttribute
  public DBType dbType = DBType.MSSQL;

  @XmlAttribute
  public boolean useJDBUrl = false;

  @XmlAttribute
  public boolean useTrustedConnection = false;

  @XmlAttribute
  public String jdbcURL = "";

  // only for the XML-Marshaler
  @SuppressWarnings("unused")
  private SQLConfig() {
  }

  public SQLConfig(String aUser, String aDatabase, String aPassword, String anSQLServer,
          DBType aDbType) {
    this(aUser, aDatabase, aPassword, anSQLServer, aDbType, false);
  }

  public SQLConfig(String aUser, String aDatabase, String aPassword, String anSQLServer,
          DBType aDbType, boolean aUseTrustedConnectionFlag) {
    user = aUser;
    database = aDatabase;
    password = aPassword;
    sqlServer = anSQLServer;
    dbType = aDbType;
    useTrustedConnection = aUseTrustedConnectionFlag;
  }

  public SQLConfig(SQLConfig anotherConfig) {
    this(anotherConfig.user, anotherConfig.database, anotherConfig.password,
            anotherConfig.sqlServer, anotherConfig.dbType, anotherConfig.useTrustedConnection);
  }

  public static DBType guessDatabseType() {
    String computerName = null;
    try {
      computerName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
    }
    if ((computerName != null) && computerName.matches("wklw.*")) {
      return DBType.MSSQL;
    } else {
      return DBType.MySQL;
    }
  }

  public boolean isValid() {
    if ((sqlServer != null) && (!sqlServer.isEmpty())) {
      return true;
    } else {
      return false;
    }
  }

  public static SQLConfig read(File aFile) {
    String text, user = null, db = null, server = null, pass = null;
    DBType dbType = DBType.MySQL;
    try {
      text = FileUtilsUniWue.file2String(aFile);
      String[] lines = text.split("\n");
      for (String aLine : lines) {
        String[] tokens = aLine.split("\t");
        if (tokens[0].equals("user")) {
          user = tokens[1].trim();
        } else if (tokens[0].equals("database")) {
          db = tokens[1].trim();
        } else if (tokens[0].equals("password")) {
          pass = tokens[1].trim();
        } else if (tokens[0].equals("sqlServer")) {
          server = tokens[1].trim();
        } else if (tokens[0].equals("dbType")) {
          String dbTypeString = tokens[1].trim();
          dbType = DBType.valueOf(dbTypeString);
        }
      }
      SQLConfig result = new SQLConfig(user, db, pass, server, dbType);
      return result;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((database == null) ? 0 : database.hashCode());
    result = prime * result + ((dbType == null) ? 0 : dbType.hashCode());
    result = prime * result + ((jdbcURL == null) ? 0 : jdbcURL.hashCode());
    result = prime * result + ((password == null) ? 0 : password.hashCode());
    result = prime * result + ((sqlServer == null) ? 0 : sqlServer.hashCode());
    result = prime * result + (useJDBUrl ? 1231 : 1237);
    result = prime * result + (useTrustedConnection ? 1231 : 1237);
    result = prime * result + ((user == null) ? 0 : user.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SQLConfig other = (SQLConfig) obj;
    if (database == null) {
      if (other.database != null)
        return false;
    } else if (!database.equals(other.database))
      return false;
    if (dbType != other.dbType)
      return false;
    if (jdbcURL == null) {
      if (other.jdbcURL != null)
        return false;
    } else if (!jdbcURL.equals(other.jdbcURL))
      return false;
    if (password == null) {
      if (other.password != null)
        return false;
    } else if (!password.equals(other.password))
      return false;
    if (sqlServer == null) {
      if (other.sqlServer != null)
        return false;
    } else if (!sqlServer.equals(other.sqlServer))
      return false;
    if (useJDBUrl != other.useJDBUrl)
      return false;
    if (useTrustedConnection != other.useTrustedConnection)
      return false;
    if (user == null) {
      if (other.user != null)
        return false;
    } else if (!user.equals(other.user))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "" + "Server: " + sqlServer + "\n" + "Database:" + database + "\n" + "User:" + user
            + "\n" + "DBType:" + dbType.toString();
  }

  public String ToXML() {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(SQLConfig.class);
      Marshaller m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      StringWriter stream = new StringWriter();
      m.marshal(this, stream);
      String text = stream.getBuffer().toString();
      return text;
    } catch (JAXBException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static SQLConfig tryUnmarshal(String xml) {
    SQLConfig result = null;
    try {
      JAXBContext context = JAXBContext.newInstance(SQLConfig.class);
      Unmarshaller m = context.createUnmarshaller();
      StringReader reader = new StringReader(xml);
      result = (SQLConfig) m.unmarshal(reader);
      return result;
    } catch (JAXBException e) {
      e.printStackTrace();
      return null;
    }
  }

  public String getSQLFactoryClassName(String defaultValue) throws SQLException {
    String result = defaultValue;
    if (dbType == DBType.MSSQL) {
      result = "de.uniwue.misc.sql.MSSQL.MSSQLFactory";      
    } else if (dbType == DBType.MySQL) {
      result = "de.uniwue.misc.sql.MySQL.MySQLFactory";            
    } else if (dbType == DBType.MaxDB) {
      result = "de.uniwue.misc.sql.MAXDB.MAXDBFactory";            
    }
    return result;
  }
  
}
