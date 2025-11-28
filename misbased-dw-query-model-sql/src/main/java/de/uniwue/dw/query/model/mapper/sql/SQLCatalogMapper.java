package de.uniwue.dw.query.model.mapper.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.core.model.data.CatalogEntry;
import de.uniwue.dw.core.model.manager.CatalogManager;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.model.client.GUIClientException;
import de.uniwue.dw.query.model.lang.QueryAttribute;
import de.uniwue.dw.query.model.lang.QueryRoot;
import de.uniwue.dw.query.model.mapper.ICatalogMapper;
import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;

public abstract class SQLCatalogMapper extends DatabaseManager implements ICatalogMapper {
  
  private static Logger logger=LogManager.getLogger(SQLCatalogMapper.class);

  public SQLCatalogMapper(SQLManager aSqlManager) throws SQLException {
    super(aSqlManager);
    createSQLTables();
    if (tableIsEmpty())
      insertOneToOneMapping();
  }

  private void insertOneToOneMapping() throws SQLException {
    logger.debug("Inserting 1 to 1 mapping.");
    addLocalAttrIdToGlobal(DwClientConfiguration.getInstance().getCatalogManager().getRoot());
  }

  public boolean tableIsEmpty() throws SQLException {
    String sql = getTableIsEmptySQL();
    Statement stmt = sqlManager.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    boolean tableIsEmpty = true;
    if (rs.next())
      tableIsEmpty = false;
    rs.close();
    stmt.close();
    return tableIsEmpty;
  }

  protected abstract String getTableIsEmptySQL();

  @Override
  public void queryRootMapper(QueryRoot queryRoot) throws GUIClientException, SQLException {

    for (QueryAttribute queryAttribute : queryRoot.getAttributesRecursive()) {
      queryAttribute
              .setCatalogEntry(getLocalCatalogEntryByMaster(queryAttribute.getCatalogEntry()));
    }
  }

  @Override
  public String getTableName() {
    return IDwSqlSchemaConstant.T_CATALOG_MAPPER;
  }

  @Override
  protected void readResult(ResultSet resultSet) throws SQLException {
  }

  @Override
  protected abstract String getCreateTableString();

  public CatalogEntry getLocalCatalogEntryByMaster(CatalogEntry masterCatalogEntry)
          throws SQLException, GUIClientException {
    String sql = "select * from " + getTableName() + " where masterExtId=? and masterProject=? ";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, masterCatalogEntry.getExtID());
    stmt.setString(2, masterCatalogEntry.getProject());
    ResultSet rs = stmt.executeQuery();

    String extId = "";
    String project = "";
    boolean hasMapping = false;

    if (rs.next()) {
      hasMapping = true;
      extId = rs.getString("localExtId");
      project = rs.getString("localProject");
    }
    rs.close();
    stmt.close();

    if (!hasMapping || (extId.equals("") || project.equals("")))
      throw new GUIClientException(
              "Für \"" + masterCatalogEntry.getName() + "\" ist kein Mapping Element vorhanden!");

    CatalogManager catalogManager = DwClientConfiguration.getInstance().getCatalogManager();
    try {
      return catalogManager.getEntryByRefID(extId, project);
    } catch (SQLException e) {
      throw new GUIClientException(
              "Für \"" + masterCatalogEntry.getName() + "\" ist kein Mapping Element vorhanden!");
    }
  }

  private void setAttrRelation(String masterExtId, String masterProject, String localExtId,
          String localProject) throws SQLException {
    String sql = "Insert INTO " + getTableName() + " (masterExtId, masterProject, localExtId, localProject) VALUES(?,?,?,?)";
    PreparedStatement stmt = sqlManager.createPreparedStatement(sql);
    stmt.setString(1, masterExtId);
    stmt.setString(2, masterProject);
    stmt.setString(3, localExtId);
    stmt.setString(4, localProject);
    stmt.execute();
    stmt.close();
    commit();
  }

  private void addLocalAttrIdToGlobal(CatalogEntry catalogEntry) throws SQLException {
    String extId = catalogEntry.getExtID();
    String project = catalogEntry.getProject();
    setAttrRelation(extId, project, extId, project);
    for (CatalogEntry childEntry : catalogEntry.getChildren()) {
      addLocalAttrIdToGlobal(childEntry);
    }
  }

//  public static void main(String[] args)
//          throws SQLException, GUIClientException, IOException, ConfigException {
//    DwClientConfiguration.loadProperties(
//            new File("E:\\GIT\\misbased-padawan-rest\\src\\main\\webapp\\WEB-INF\\server.props"));
//    SQLCatalogMapper mapper = new SQLCatalogMapper(
//            SQLPropertiesConfiguration.getInstance().getSQLManager());
//    IGUIClient guiClient = Authenticator.getInstance().getGUIClient();
//    mapper.addLocalAttrIdToGlobal(guiClient.getCatalogClientProvider().getRoot());
//  }

}
