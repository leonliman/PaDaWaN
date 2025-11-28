package de.uniwue.dw.query.solr.preprocess.util;

import de.uniwue.dw.core.sql.IDwSqlSchemaConstant;
import de.uniwue.dw.query.solr.client.ISolrConstants;

/*
* wer diese Klasse wieder nutzen möchte muss sich mit der Neuerung auseinandersetzen, 
* dass der BulkInsert nun an einen DatabaseMAnager gekoppelt ist. Das müsste man hier dann ein wenig umbauen.
 */
@Deprecated
public class ConsistencyChecker implements IDwSqlSchemaConstant,ISolrConstants {

//  public static void findMissingCaseIDs() throws SQLException, SolrServerException, IOException {
//    long start = System.currentTimeMillis();
//    SQLPropertiesConfiguration.getInstance().getSQLManager().dropTable("SolrIndex");
//    String sql = "CREATE TABLE [dbo].[SolrIndex]([caseID] [bigint]NOT NULL, PRIMARY KEY CLUSTERED "
//            + "([caseID] ASC)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = ON, "
//            + "ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]) ON [PRIMARY]";
//    SQLPropertiesConfiguration.getInstance().getSQLManager().executeSQL(sql);
//    Statement stmt = SQLPropertiesConfiguration.getInstance().getSQLManager().createStatement();
//    sql = "select min(caseid), max(caseid) from " + T_IMPORT_CASES;
//    ResultSet rs = stmt.executeQuery(sql);
//    if (rs.next()) {
//      long minCaseID = rs.getLong(1);
//      long maxCaseID = rs.getLong(2);
//      long idRangePerBatch = 1000000000000000L;
//      int dbInsertBatchSize = 1000000;
//      long curID = minCaseID;
//      BulkInserter inserter = new BulkInserter("SolrIndex",
//              DwClientConfiguration.getInstance()
//                      .getParameter(ISqlConfigKeys.PARAM_SQL_BULK_IMPORT_DIR),
//                      SQLPropertiesConfiguration.getInstance().getSQLManager());
//      int idsInBulkInserter = 0;
//      while (curID <= maxCaseID) {
//        String queryText = "id:[" + curID + " TO " + (curID + idRangePerBatch) + "]";
//        System.out.print(queryText);
//        curID += idRangePerBatch;
//        SolrQuery query = new SolrQuery(queryText);
//        query.addField("id");
//        query.setRows(MAX_ROWS);
//        SolrClient server = DWSolrConfig.getSolrManager().getServer();
//        QueryResponse rsp = server.query(query);
//        int idsInBatch = 0;
//        for (SolrDocument doc : rsp.getResults()) {
//          inserter.addRow(doc.getFieldValue("id"));
//          idsInBulkInserter++;
//          idsInBatch++;
//        }
//        System.out.println(" #idsInSolrBatch: " + idsInBatch);
//        if (idsInBulkInserter > dbInsertBatchSize) {
//          System.out.print("Inserting bulk part..");
//          inserter.insertPart();
//          idsInBulkInserter = 0;
//          System.out.println("Done");
//        }
//      }
//      inserter.executeInsert();
//      inserter.close();
//    }
//    long end = System.currentTimeMillis();
//    System.out.println("Duration: " + (end - start));
//  }
}
