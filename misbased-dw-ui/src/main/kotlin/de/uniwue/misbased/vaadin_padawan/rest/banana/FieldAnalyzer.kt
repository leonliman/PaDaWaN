package de.uniwue.misbased.vaadin_padawan.rest.banana

import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.dw.query.model.client.GUIClientException
import de.uniwue.dw.query.solr.SolrUtil
import de.uniwue.dw.query.solr.client.ISolrConstants.*
import de.uniwue.dw.solr.api.DWSolrConfig
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.SolrQuery.ORDER
import org.apache.solr.client.solrj.SolrServerException
import org.apache.solr.client.solrj.response.FieldStatsInfo
import org.apache.solr.common.SolrDocumentList
import java.io.IOException
import java.sql.SQLException
import java.util.*

class FieldAnalyzer {

    class FieldStats {
        internal var entry: CatalogEntry? = null

        internal var minString: String? = null
        internal var maxString: String? = null
        internal var fieldName: String? = null
        internal var entryName: String? = null

        internal var minDouble: Double = 0.toDouble()
        internal var maxDouble: Double = 0.toDouble()
        internal var mean: Double = 0.toDouble()
        internal var std: Double = 0.toDouble()

        internal var minDate: Date? = null
        internal var maxDate: Date? = null
    }

    companion object {

        @Throws(SolrServerException::class, SQLException::class, IOException::class, GUIClientException::class)
        fun analyzeField(attrID: Int): FieldStats {
            val entry = PaDaWaNConnector.getCatalogManager().getEntryByID(attrID)
            return analyzeField(entry)

        }

        @Throws(SolrServerException::class, SQLException::class, IOException::class)
        fun analyzeFieldMeasureTime(entry: CatalogEntry): FieldStats {
            return queryMinMayMeasureDateForEntry(entry)
        }

        @Throws(SolrServerException::class, IOException::class)
        private fun queryMinMayMeasureDateForEntry(entry: CatalogEntry): FieldStats {
            val field = FieldStats()
            field.fieldName = "date_measure_time"
            field.entry = entry
            field.entryName = entry.name
            val q = SolrQuery("*:*")
            val docTypeQuery = SolrUtil.createDocTypeQuery(DOC_TYPE.CASE)
            q.addFilterQuery(docTypeQuery)
            val layerQuery = "$SOLR_FIELD_PARENT_CHILD_LAYER:$CHILD_LAYER"
            q.addFilterQuery(layerQuery)
            q.addFilterQuery(FIELD_EXISTANCE_FIELD + ":" + SolrUtil.getSolrID(entry))
            q.setSort("date_measure_time", ORDER.asc)
            val server = DWSolrConfig.getInstance().solrManager.server
            var response = server.query(q, DWSolrConfig.getSolrMethodToUse())
            var results: SolrDocumentList? = response.results
            if (results != null) {
                field.minDate = results[0].getFieldValue("date_measure_time") as Date
            }
            q.setSort("date_measure_time", ORDER.desc)
            response = server.query(q, DWSolrConfig.getSolrMethodToUse())
            results = response.results
            if (results != null) {
                field.maxDate = results[0].getFieldValue("date_measure_time") as Date
            }
            return field
        }

        @Throws(SolrServerException::class, SQLException::class, IOException::class)
        fun analyzeField(entry: CatalogEntry): FieldStats {
            val field = FieldStats()

            field.entry = entry
            field.entryName = entry.name

            val solrFieldName: String = SolrUtil.getSolrFieldName(entry)

            field.fieldName = solrFieldName

            if (entry.dataType == CatalogEntryType.DateTime
                || entry.dataType == CatalogEntryType.Number
            ) {
                val queryStats = queryStats(solrFieldName)
                field.minString = queryStats.min.toString()
                field.maxString = queryStats.max.toString()
                if (entry.dataType == CatalogEntryType.Number) {
                    field.minDouble = queryStats.min as Double
                    field.maxDouble = queryStats.max as Double
                    field.mean = queryStats.mean as Double
                    field.std = queryStats.stddev as Double
                } else if (entry.dataType == CatalogEntryType.DateTime) {
                    field.minDate = queryStats.min as Date
                    field.maxDate = queryStats.max as Date
                }
            }
            return field
        }

        @Throws(SolrServerException::class, IOException::class)
        private fun queryStats(field: String): FieldStatsInfo {
            val solrManager = DWSolrConfig.getInstance().solrManager
            val server = solrManager.server
            val query = SolrQuery("*:*")
            query.setGetFieldStatistics(true)
            query.setGetFieldStatistics(field)
            val response = server.query(query, DWSolrConfig.getSolrMethodToUse())

            return response.fieldStatsInfo[field]!!
        }
    }
}