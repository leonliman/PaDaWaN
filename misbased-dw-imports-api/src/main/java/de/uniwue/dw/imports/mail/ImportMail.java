package de.uniwue.dw.imports.mail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.api.IDwImportsConfigurationKeys;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.misc.sql.SQLPropertiesConfiguration;

public class ImportMail implements IDwImportsConfigurationKeys {

  static String T_ERROR_LOG = "DWImportLog";

  DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

  DecimalFormat decf = new DecimalFormat("#");

  Map<String, ImportResult> importResults = new HashMap<String, ImportResult>();

  private static final String EMAIL_SUBJECT_TEMPLATE = "the-dw-imperator - %s";

  @SuppressWarnings("unused")
  private static final String SQL_STATEMENT_FIRST = "("
          + " select count(*) as c, project as domain from" + " dwinfo, dwcatalog" + " where"
          + " dwinfo.AttrID = DWCatalog.AttrID" + " and"
          + " datediff(hh, ImportTime, CURRENT_TIMESTAMP) < 24" + " group by project" + " )"
          + " UNION" + " (" + " select count(*) as c, ExtID as domain from dwinfo, DWCatalog"
          + " where dwinfo.AttrID = DWCatalog.AttrID" + " and"
          + " datediff(hh, ImportTime, CURRENT_TIMESTAMP) < 24" + " and ExtID = 'EKG'"
          + " group by ExtID" + " )";

  private static final String SQL_ERROR_STATEMENT = "( select project, " + "       errorType, "
          + "       count(ID) as count , " + "       min(filename) as firstFile, "
          + "       max(filename) as lastFile, " + "       min(message) as firstMessage, "
          + "       max(message) as lastMessage " + " from " + T_ERROR_LOG
          + " group by project, errorType )";

  static int CNT_LAST_MONTH = 12;

  private static final String SQL_DETAILED_STATEMENT = ""
          + "SELECT  c.Project                                                          "
          + "        ,measdate                                                          "
          + "        ,sum(i.cnt) as cnt                                                 "
          + "FROM (                                                                     "
          + "        SELECT AttrID                                                      "
          + "               ,cast(MeasureTime AS DATE) AS measdate                      "
          + "               , count(*) as cnt                                           "
          + "        FROM   DWInfo                                                      "
          + "        WHERE  MeasureTime > DATEADD(MONTH, -" + CNT_LAST_MONTH + ", GETDATE())"
          + "        group by AttrID, cast(MeasureTime AS DATE)                         "
          + "     ) AS i LEFT JOIN DWCatalog AS c ON i.AttrID = c.AttrID                "
          + "GROUP BY c.Project                                                         "
          + "         ,measdate                                                         "
          + "ORDER BY c.Project  ,measdate                                              ";

  private List<String> infos;

  public ImportMail() throws EmailException {

    List<String> infos = new ArrayList<String>();
    infos.add(DwClientConfiguration.getInstance().getParameter(PARAM_EMAIL_SERVER));
    infos.add(DwClientConfiguration.getInstance().getParameter(PARAM_EMAIL_PORT));
    infos.add(DwClientConfiguration.getInstance().getParameter(PARAM_EMAIL_USER));
    infos.add(DwClientConfiguration.getInstance().getParameter(PARAM_EMAIL_PASSWORD));
    infos.add(DwClientConfiguration.getInstance().getParameter(PARAM_EMAIL_ADDRESS));
    infos.add(DwClientConfiguration.getInstance().getParameter(PARAM_EMAIL_RECEIVER));

    // check whether all necessary parameters have been set
    for (int i = 0; i < infos.size(); i++) {
      if (infos.get(i) == null || infos.get(i).isEmpty()) {
        throw new EmailException("No email sent (not enough parameters set)");
      }
    }
    this.infos = infos;
  }

  public static void sendNotification() throws ImportException {
    try {
      new ImportMail().sendEmail();
    } catch (EmailException e) {
      throw new ImportException(ImportExceptionType.IO_ERROR, e);
    } catch (SQLException e) {
      throw new ImportException(ImportExceptionType.SQL_ERROR, e);
    } catch (ParseException e) {
      throw new ImportException(ImportExceptionType.DATA_MISMATCH, e);
    }
  }

  public void sendEmail() throws EmailException, SQLException, ParseException {
    // String importMsg = getInformationFromSqlAsHtml(SQL_STATEMENT_FIRST);
    String errorMsg = getInformationFromSqlAsHtml(SQL_ERROR_STATEMENT);
    String overview = getExportImportOverview();
    String text = "<html>" + "<h1>Imported data</h1>" + overview + "<br><br>"
            + "<h1>Erroneous imports</h1>" + errorMsg + "</html>";

    if (infos.size() < 6) {
      throw new EmailException(new IllegalArgumentException("Not enough information given for "
              + "sending the email!"));
    }
    HtmlEmail email = new HtmlEmail();
    email.setHostName(infos.get(0));
    email.setSmtpPort(Integer.valueOf(infos.get(1)));
    email.setAuthenticator(new DefaultAuthenticator(infos.get(2), infos.get(3)));
    email.setSSLOnConnect(Integer.valueOf(infos.get(1)) == 25 ? false : true);
    email.setFrom(infos.get(4));
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
    String date = LocalDateTime.now().format(formatter);
    String subject = String.format(EMAIL_SUBJECT_TEMPLATE, date);
    email.setSubject(subject);
    email.setHtmlMsg(text);
    email.addTo(infos.get(5));
    email.send();
    ImportLogManager.info("Email sent to " + infos.get(5));
  }

  /**
   * Executes the given statement and returns the results in a html table (begins and ends with the
   * table tags).
   * 
   * @param query
   * @return
   * @throws SQLException
   * @throws ParseException
   */
  public String getExportImportOverview() throws SQLException, ParseException {

    updateImportResults();

    updateMetrics();

    String result = "<style type='text/css'>                                        "
            + "<!--                                                                 "
            + " table {border-collapse: collapse;}                                  "
            + " table, th, td {border: 1px solid black;padding: 4px;text-align: left;}"
            + ".err {background-color:red;font-weight:bold;}                        "
            + ".warn {background-color:yellow;font-weight:bold;}                    "
            + "-->                                                                  "
            + "</style>                                                             ";

    result += "<table>";

    result += "<tr>";
    result += "<th width='10%'>Domain</th>";
    result += "<th width='10%'>Days since last import</th>";
    result += "<th width='10%'>#Imported attributes of yesterday</th>";
    result += "<th width='10%'>Mean  &plusmn; StDev of #imported attributes/day (last "
            + CNT_LAST_MONTH + " month w/o yesterday)</th>";
    result += "<th>Errors</th>";
    result += "</tr>";

    for (String projectKey : importResults.keySet()) {
      ImportResult res = importResults.get(projectKey);
      result += "<tr>";
      result += "<td>" + res.project + "</td>";

      result += "<td " + (res.lastImportError ? "class='err'" : "") + ">";
      result += res.lastImportSinceDays;
      result += "</td>";

      result += "<td "
              + (res.yesterdayCount == 0 ? "class='err'" : res.yesterdayWarn ? "class='warn'" : "")
              + ">";
      result += res.yesterdayCount;
      result += "</td>";

      result += "<td>";
      result += decf.format(res.mean) + " &plusmn; " + decf.format(res.stdev);
      result += "</td>";

      result += "<td " + (res.stateError || res.detailedStateError ? "class='err'" : "") + ">";
      String tmpErr = res.state.replace("\n", "<br>");
      if (tmpErr.length() > 0)
        tmpErr += "<br>";
      tmpErr += res.detailedState.replace("\n", "<br>");
      result += tmpErr;
      result += "</td>";

      result += "</tr>";
    }
    result += "</table>";

    return result;
  }

  private void updateMetrics() {
    for (String projectKey : importResults.keySet()) {
      ImportResult res = importResults.get(projectKey);

      Collections.sort(res.detail, new ImportDetailComparator());

      ImportDetail lastDet = null;

      for (int i = 0; i < res.detail.size(); i++) {
        ImportDetail det = res.detail.get(i);

        if (lastDet != null) {
          Long diffDays = (det.date.getTime() - lastDet.date.getTime()) / (24 * 60 * 60 * 1000);
          if (diffDays > 1) {
            res.detailedState += (diffDays - 1) + " days are missing between "
                    + df.format(lastDet.date) + " and " + df.format(det.date) + "\n";
          }
        }
        if (det.daysSince > 1) {
          res.prevCounts.add(det.count);
        } else if (det.daysSince == 1) {
          res.yesterdayCount = det.count;
        }

        lastDet = det;
      }

      res.mean = 0.0;
      for (double a : res.prevCounts)
        res.mean += a;
      res.mean = res.mean / res.prevCounts.size();

      res.variance = 0.0;
      for (double a : res.prevCounts)
        res.variance += (res.mean - a) * (res.mean - a);
      res.variance = res.variance / res.prevCounts.size();

      res.stdev = Math.sqrt(res.variance);

      if (res.yesterdayCount == 0 || res.yesterdayCount < (res.mean - res.stdev * 2)
              || res.yesterdayCount > (res.mean + res.stdev * 2)) {
        res.yesterdayWarn = true;
      } else {
        res.yesterdayWarn = false;
      }

      if (res.detail.size() > 0) {
        res.lastImportSinceDays = res.detail.get(res.detail.size() - 1).daysSince;
        res.lastImportCount = res.detail.get(res.detail.size() - 1).count;
      }

      if (res.lastImportSinceDays > 1) {
        res.lastImportError = true;
      }

      if (res.lastImportCount < res.mean - res.stdev * 2
              || res.lastImportCount > res.mean + res.stdev * 2) {
        res.lastImportCountWarn = true;
      }

      if (res.state.length() > 0) {
        res.stateError = true;
      }
      if (res.detailedState.length() > 0) {
        res.detailedStateError = true;
      }
    }

  }

  private void updateImportResults() throws SQLException, ParseException {
    importResults = new HashMap<String, ImportResult>();

    List<String> expDomains = DwClientConfiguration.getInstance().getArrayParameter(
            PARAM_EXPECTED_DOMAINS);

    String lastProject = null;

    PreparedStatement statement = SQLPropertiesConfiguration.getInstance().getSQLManager()
            .createPreparedStatement(SQL_DETAILED_STATEMENT);
    ResultSet resultSet = statement.executeQuery();

    while (resultSet.next()) {

      String project = resultSet.getString("Project");
      String datStr = resultSet.getString("measdate");
      String cntStr = resultSet.getString("cnt");

      ImportDetail det = new ImportDetail();
      det.date = df.parse(datStr);
      det.count = Integer.parseInt(cntStr);
      long diff = new Date().getTime() - det.date.getTime();
      det.daysSince = diff / (24 * 60 * 60 * 1000);

      ImportResult res;
      if (lastProject == null || !lastProject.equals(project)) {

        res = new ImportResult();
        res.project = project;
        importResults.put(project, res);

        if (expDomains.contains(res.project)) {
          expDomains.remove(res.project);
        } else {
          res.state += "Project not in required imports list";
        }
      } else {
        res = importResults.get(project);
      }

      if (det.daysSince >= 0) {
        res.detail.add(det);
      } else {
        res.detailErroneous.add(det);
      }
      lastProject = project;
    }

    for (String project : expDomains) {
      ImportResult res = new ImportResult();
      res.project = project;
      res.state = "No imported data available";
      importResults.put(project, res);
    }

  }

  /**
   * Executes the given statement and returns the results in a html table (begins and ends with the
   * table tags).
   * 
   * @param query
   * @return
   * @throws SQLException
   */
  private String getInformationFromSqlAsHtml(String query) throws SQLException {
    String result = "<table>";
    PreparedStatement statement = SQLPropertiesConfiguration.getInstance().getSQLManager()
            .createPreparedStatement(query);
    ResultSet resultSet = statement.executeQuery();
    /*
     * Display results in a html table
     */
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    // get table headers
    result += "<tr>";
    for (int i = 1; i <= columnCount; i++) {
      result += "<th>" + metaData.getColumnLabel(i) + "</th>";
    }
    result += "</tr>";
    // get data
    while (resultSet.next()) {
      result += "<tr>";
      for (int i = 1; i <= columnCount; i++) {
        result += "<td>" + resultSet.getString(i) + "</td>";
      }
      result += "</tr>";
    }
    result += "</table>";
    return result;
  }
}
