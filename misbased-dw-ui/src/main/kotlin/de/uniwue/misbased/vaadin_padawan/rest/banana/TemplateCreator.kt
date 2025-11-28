package de.uniwue.misbased.vaadin_padawan.rest.banana

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.dw.query.model.client.GUIClientException
import de.uniwue.dw.query.solr.SolrUtil
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import de.uniwue.misbased.vaadin_padawan.rest.banana.FieldAnalyzer.FieldStats
import org.apache.solr.client.solrj.SolrServerException
import java.io.*
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.util.*

object TemplateCreator {

    private const val PARAM_TITLE = "\$TITLE$"

    private const val PARAM_TEXT_FIELD = "\$TEXT_FIELD$"

    private const val PARAM_DATE_FIELD = "\$DATE_FIELD$"

    private const val PARAM_DATE_FROM = "\$DATE_FROM$"

    private const val PARAM_TIME_WINDOW_DATE_FROM = "\$TW_DATE_FROM$"

    private const val PARAM_TIME_WINDOW_DATE_TO = "\$TW_DATE_TO$"

    private const val PARAM_DATE_TO = "\$DATE_TO$"

    private const val PARAM_NUMERIC_FIELD = "\$NUMERIC_FIELD$"

    private const val PARAM_NUMERIC_MIN = "\$NUMERIC_MIN$"

    private const val PARAM_NUMERIC_MAX = "\$NUMERIC_MAX$"

    private const val PARAM_SERVER = "\$SERVER$"

    private const val PARAM_CONTAINING_FIELD_ID = "\$CONTAINING_FIELD_ID$"

    private const val PARAM_TIME_SERIES_X_AXIS_LABEL = "\$TIME_SERIES_X_AXIS_LABEL$"

    private const val PARAM_TIME_SERIES_Y_AXIS_LABEL = "\$TIME_SERIES_Y_AXIS_LABEL$"

    private const val PARAM_TIME_SERIES_TITLE = "\$TIME_SERIES_TITLE$"

    private const val PARAM_RANGE_QUERY_TITLE = "\$RANGE_QUERY_TITLE$"

    private const val PARAM_RANGE_QUERY_X_AXIS_LABEL = "\$RANGE_QUERY_X_AXIS_LABEL$"

    private const val PARAM_RANGE_QUERY_Y_AXIS_LABEL = "\$RANGE_QUERY_Y_AXIS_LABEL$"

    private const val PARAM_TIME_WINDOW_TITLE = "\$TIME_WINDOW_TITLE$"

    private const val PARAM_HITS_INFOS_FILTER_TITLE = "\$HITS_INFOS_FILTER_TITLE$"

    private const val PARAM_HITS_INFOS_ABSOLUT_TITLE = "\$HITS_INFOS_ABSOLUT_TITLE$"

    private const val PARAM_HITS_CASES_FILTER_TITLE = "\$HITS_CASES_FILTER_TITLE$"

    private const val PARAM_HITS_CASES_ABSOLUT_TITLE = "\$HITS_CASES_ABSOLUT_TITLE$"

    private const val PARAM_TAG_CLOUD_TITLE = "\$TAG_CLOUD_TITLE$"

    private var date: TimelineDate? = null

    private class TimelineDate {

        var dateField: String? = null

        var name: String? = null

        var dateFrom: String? = null
        var dateTo: String? = null

        var windowDateFrom: String? = null
        var windowDateTo: String? = null

        companion object {

            private const val TIME_WINDOW_DATE_FORMAT = "MM/dd/yyyy hh:mm:ss"

            private const val DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss.000'Z'"

            private val dateFormat = SimpleDateFormat(DATE_FORMAT)

            private val windowDateFormat = SimpleDateFormat(TIME_WINDOW_DATE_FORMAT)

            @Throws(SolrServerException::class, SQLException::class, IOException::class, GUIClientException::class)
            fun create(documentTime: CatalogEntry): TimelineDate {
                val dateFieldStats = FieldAnalyzer.analyzeField(documentTime.attrId)
                return create(dateFieldStats)
            }

            fun create(dateFieldStats: FieldStats): TimelineDate {
                val t = TimelineDate()
                t.dateField = dateFieldStats.fieldName
                t.name = dateFieldStats.entryName

                val min = Date(Math.min(dateFieldStats.maxDate!!.time, System.currentTimeMillis()))
                t.dateFrom = dateFormat.format(dateFieldStats.minDate)
                t.dateTo = dateFormat.format(min)

                t.windowDateFrom = windowDateFormat.format(dateFieldStats.minDate)
                t.windowDateTo = windowDateFormat.format(dateFieldStats.maxDate)
                return t
            }
        }

    }

    init {
        PaDaWaNConnector.getCatalogManager()
        val documentTime = DwClientConfiguration.getInstance().specialCatalogEntries.documentTime
        if (documentTime != null && documentTime != DwClientConfiguration.getInstance().catalogManager.root) {
            date = TimelineDate.create(documentTime)
        }
    }

    @Throws(IOException::class, SolrServerException::class, SQLException::class)
    fun getDashboard(catalogEntry: CatalogEntry, server: String): String {
        val field = FieldAnalyzer.analyzeField(catalogEntry)
        val template = getTemplate(field, server)
        return getSolrResponse(field, template)
    }

    @Throws(IOException::class, SolrServerException::class, SQLException::class)
    fun getTemplate(field: FieldStats, server: String): String {
        var templateName = "bool.json"
        when {
            field.entry!!.dataType == CatalogEntryType.Number -> templateName = "number.json"
            field.entry!!.dataType == CatalogEntryType.Text -> templateName = "text.json"
            field.entry!!.dataType == CatalogEntryType.DateTime -> templateName = "date.json"
        }
        var template = loadFile(templateName)

        template = template.replace(PARAM_TITLE, field.entry!!.name)
        template = template.replace(PARAM_SERVER, server)

        template = template.replace(PARAM_TIME_WINDOW_TITLE, "Auswahl der Zeit")
        template = template.replace(PARAM_HITS_INFOS_FILTER_TITLE, "Gefilterte Infos im DWH")
        template = template.replace(PARAM_HITS_INFOS_ABSOLUT_TITLE, "Infos im DWH")
        template = template.replace(PARAM_HITS_CASES_FILTER_TITLE, "Gefilterte Fälle im DWH")
        template = template.replace(PARAM_HITS_CASES_ABSOLUT_TITLE, "Fälle im DWH")

        var templateDate = date

        if (field.entry!!.dataType == CatalogEntryType.DateTime) {
            templateDate = TimelineDate.create(field)
        }
        if (templateDate == null) {
            val measureTimeFieldStats = FieldAnalyzer.analyzeFieldMeasureTime(field.entry!!)
            templateDate = TimelineDate.create(measureTimeFieldStats)
        }

        template = template.replace(PARAM_DATE_FIELD, templateDate.dateField!!)
        template = template.replace(PARAM_DATE_FROM, templateDate.dateFrom!!)
        template = template.replace(PARAM_DATE_TO, templateDate.dateTo!!)

        template = template.replace(PARAM_TIME_WINDOW_DATE_FROM, templateDate.windowDateFrom!!)
        template = template.replace(PARAM_TIME_WINDOW_DATE_TO, templateDate.windowDateTo!!)

        val timeLineTitle = "Zeitverlauf des Attributs: " + field.entry!!.name
        val timeLineXAxisLabel = templateDate.name
        val timeLineYAxisLabel = "Anzahl"

        template = template.replace(PARAM_TIME_SERIES_TITLE, timeLineTitle)
        template = template.replace(PARAM_TIME_SERIES_X_AXIS_LABEL, timeLineXAxisLabel!!)
        template = template.replace(PARAM_TIME_SERIES_Y_AXIS_LABEL, timeLineYAxisLabel)

        val solrID = SolrUtil.getSolrID(field.entry!!)
        template = template.replace(PARAM_CONTAINING_FIELD_ID, solrID)

        if (field.entry!!.dataType == CatalogEntryType.Number) {
            val generatedMin = field.mean - 3 * field.std
            val generatedMax = field.mean + 3 * field.std

            val min = Math.max(field.minDouble, generatedMin)
            val max = Math.min(field.maxDouble, generatedMax)

            template = template.replace(PARAM_NUMERIC_FIELD, field.fieldName!!)
            template = template.replace(PARAM_NUMERIC_MIN, min.toString() + "")
            template = template.replace(PARAM_NUMERIC_MAX, max.toString() + "")

            val rangeQueryTitle = "Histogramm des Attributs: " + field.entry!!.name
            val rangeQueryXAxisLabel = field.entry!!.name
            val rangeQueryYAxisLabel = "Anzahl"

            template = template.replace(PARAM_RANGE_QUERY_TITLE, rangeQueryTitle)
            template = template.replace(PARAM_RANGE_QUERY_X_AXIS_LABEL, rangeQueryXAxisLabel)
            template = template.replace(PARAM_RANGE_QUERY_Y_AXIS_LABEL, rangeQueryYAxisLabel)
        } else if (field.entry!!.dataType == CatalogEntryType.Text) {
            template = template.replace(PARAM_TEXT_FIELD, field.fieldName!!)
            val tagCloudTitle = "Tag-Cloud für das Attribut: " + field.entryName!!
            template = template.replace(PARAM_TAG_CLOUD_TITLE, tagCloudTitle)
        }

        return template
    }

    @Throws(UnsupportedEncodingException::class, FileNotFoundException::class, IOException::class)
    private fun loadFile(templateName: String): String {
        val templateFile = PaDaWaNConnector.getResourceFile("/banana/$templateName")
        return templateFile.readText(Charsets.UTF_8)
    }

    @Throws(UnsupportedEncodingException::class, FileNotFoundException::class, IOException::class)
    fun getSolrResponse(field: FieldStats, dashboard: String): String {
        var solrResponse: String = loadFile("solrResponse.json")
        solrResponse = solrResponse.replace("\$NAME$", field.entry!!.name)
        val dashboardCopied = dashboard.replace("\r\n".toRegex(), " ").replace("\n".toRegex(), " ")
            .replace("\\s+".toRegex(), " ")
            .replace("\"".toRegex(), "\\\\\"")
        solrResponse = solrResponse.replace("\$DASHBOARD$", dashboardCopied)
        return solrResponse
    }
}