package de.uniwue.dw.imports.sql.mysql;

import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.imports.manager.CaseManager;
import de.uniwue.dw.imports.manager.DocManager;
import de.uniwue.dw.imports.manager.IImportsAdapterFactory;
import de.uniwue.dw.imports.manager.PatientManager;
import de.uniwue.dw.imports.manager.adapter.ICaseImportAdapter;
import de.uniwue.dw.imports.manager.adapter.IDBImportLogAdapter;
import de.uniwue.dw.imports.manager.adapter.IDocImportAdapter;
import de.uniwue.dw.imports.manager.adapter.IHDP_TableMappingsAdapter;
import de.uniwue.dw.imports.manager.adapter.IImportLogHandlerAdapter;
import de.uniwue.dw.imports.manager.adapter.IPatientImportAdapter;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;
import de.uniwue.dw.imports.manager.adapter.ISourceTablesAdapter;
import de.uniwue.dw.imports.sql.SQLDBImportLogAdapter;
import de.uniwue.dw.imports.sql.SQLSourceTablesAdapter;
import de.uniwue.dw.imports.sql.SQL_HDP_TableMappingsAdapter;
import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class MySQLImportsAdapterFactory implements IImportsAdapterFactory {

  @Override
  public ICaseImportAdapter createCaseImportAdapter(CaseManager caseManager) throws SQLException {
    return new MySQLCaseImportAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(), caseManager);
  }


  @Override
  public IPatientImportAdapter createPatientImportAdapter(PatientManager patientManager) throws SQLException {
    return new MySQLPatientImportAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(), patientManager);
  }


  @Override
  public IDocImportAdapter createDocImportAdapter(DocManager docManager) throws SQLException {
    return new MySQLDocImportAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(), docManager);
  }


  @Override
  public IRefIDAdapter createRefIDImportAdapter(InfoManager ainfoManager) throws SQLException {
    return new MySQLRefIDAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(), ainfoManager);
  }


  public IImportLogHandlerAdapter createLogHandlerAdapter() throws SQLException {
    return new MySQLImportLogHandler(SQLPropertiesConfiguration.getInstance().getSQLManager(),
            IDwSqlSchemaConstant.T_ERROR_LOG);
  }


  @Override
  public IHDP_TableMappingsAdapter createHDP_TableMappingsAdapter(SQLConfig sqlConfig) throws SQLException {
    return new SQL_HDP_TableMappingsAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager(sqlConfig));
  }


  @Override
  public IDBImportLogAdapter createRecordIDAdapter() throws SQLException {
    return new SQLDBImportLogAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager());
  }


  @Override
  public ISourceTablesAdapter createSourceTablesAdapter() throws SQLException {
    return new SQLSourceTablesAdapter(SQLPropertiesConfiguration.getInstance().getSQLManager());
  }

}
