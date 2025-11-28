package de.uniwue.dw.imports.manager;

import java.sql.SQLException;

import de.uniwue.dw.core.model.manager.InfoManager;
import de.uniwue.dw.imports.manager.adapter.ICaseImportAdapter;
import de.uniwue.dw.imports.manager.adapter.IDBImportLogAdapter;
import de.uniwue.dw.imports.manager.adapter.IDocImportAdapter;
import de.uniwue.dw.imports.manager.adapter.IHDP_TableMappingsAdapter;
import de.uniwue.dw.imports.manager.adapter.IImportLogHandlerAdapter;
import de.uniwue.dw.imports.manager.adapter.IPatientImportAdapter;
import de.uniwue.dw.imports.manager.adapter.IRefIDAdapter;
import de.uniwue.dw.imports.manager.adapter.ISourceTablesAdapter;
import de.uniwue.misc.sql.SQLConfig;

public interface IImportsAdapterFactory {

  IPatientImportAdapter createPatientImportAdapter(PatientManager patientManager) throws SQLException;

  ICaseImportAdapter createCaseImportAdapter(CaseManager caseManager) throws SQLException;

  IDocImportAdapter createDocImportAdapter(DocManager docManager) throws SQLException;

  IRefIDAdapter createRefIDImportAdapter(InfoManager ainfoManager) throws SQLException;

  IImportLogHandlerAdapter createLogHandlerAdapter() throws SQLException;

  IHDP_TableMappingsAdapter createHDP_TableMappingsAdapter(SQLConfig sqlConfig) throws SQLException;

  IDBImportLogAdapter createRecordIDAdapter() throws SQLException;

  ISourceTablesAdapter createSourceTablesAdapter() throws SQLException;
}
