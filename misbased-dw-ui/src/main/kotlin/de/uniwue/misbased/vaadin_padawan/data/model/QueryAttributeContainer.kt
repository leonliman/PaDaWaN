package de.uniwue.misbased.vaadin_padawan.data.model

import com.vaadin.flow.component.UI
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.data.CatalogEntryType
import de.uniwue.dw.query.model.lang.*
import de.uniwue.dw.query.model.lang.QueryAttribute.PERIOD_DELIMITER
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNQueryConnector
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.util.*

data class QueryAttributeContainer(
    var connectionType: QueryAttributeConnectionType = QueryAttributeConnectionType.NONE,
    val catalogEntry: CatalogEntry? = null,
    var queryAttributeData: QueryAttributeData = QueryAttributeData(),
    val containerID: UUID = UUID.randomUUID()
) {

    companion object {
        private val dateFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy")

        fun fromMXQLQuery(queryAttribute: QueryAttribute): QueryAttributeContainer {
            val catalogEntry = queryAttribute.catalogEntry

            val queryAttributeData = QueryAttributeData()

            queryAttributeData.showCaseID = queryAttribute.displayCaseID()
            queryAttributeData.showDocID = queryAttribute.displayDocID()
            queryAttributeData.showMeasureTime = queryAttribute.displayInfoDate()

            queryAttributeData.optional = queryAttribute.isOptional

            queryAttributeData.displayValue = queryAttribute.displayValue()
            queryAttributeData.onlyDisplayExistence = queryAttribute.isOnlyDisplayExistence
            queryAttributeData.multipleRows = queryAttribute.isMultipleRows

            queryAttributeData.contentOperator = when (queryAttribute.contentOperator) {
                ContentOperator.EMPTY -> ContentOperatorForUI.EMPTY
                ContentOperator.EQUALS -> ContentOperatorForUI.EQUALS
                ContentOperator.LESS -> ContentOperatorForUI.LESS
                ContentOperator.LESS_OR_EQUAL -> ContentOperatorForUI.LESS_OR_EQUAL
                ContentOperator.MORE -> ContentOperatorForUI.MORE
                ContentOperator.MORE_OR_EQUAL -> ContentOperatorForUI.MORE_OR_EQUAL
                ContentOperator.BETWEEN -> ContentOperatorForUI.BETWEEN
                ContentOperator.CONTAINS -> ContentOperatorForUI.CONTAINS
                ContentOperator.CONTAINS_NOT -> ContentOperatorForUI.CONTAINS_NOT
                ContentOperator.CONTAINS_POSITIVE -> ContentOperatorForUI.CONTAINS_POSITIVE
                ContentOperator.CONTAINS_NOT_POSITIVE -> ContentOperatorForUI.CONTAINS_NOT_POSITIVE
                ContentOperator.PER_YEAR -> ContentOperatorForUI.PER_YEAR
                ContentOperator.PER_MONTH -> ContentOperatorForUI.PER_MONTH
                ContentOperator.PER_INTERVALS -> ContentOperatorForUI.PER_INTERVALS
                ContentOperator.NOT_EXISTS -> ContentOperatorForUI.NOT_EXISTS
                ContentOperator.EXISTS -> ContentOperatorForUI.EXISTS
                else -> throw IllegalStateException("The content operator has an unexpected value: ${queryAttribute.contentOperator}")
            }

            queryAttributeData.reductionOperator = queryAttribute.reductionOperator
            queryAttributeData.extractionMode = queryAttribute.extractionMode

            if (queryAttribute.comment != null) {
                var commentToUse = queryAttribute.comment!!.trim()
                if (commentToUse.startsWith(PaDaWaNQueryConnector.CORRELATION_ANALYSIS_ATTRIBUTE_PREFIX)) {
                    queryAttributeData.useForCorrelationAnalysis = true
                    commentToUse =
                        commentToUse.substring(PaDaWaNQueryConnector.CORRELATION_ANALYSIS_ATTRIBUTE_PREFIX.length)
                            .trim()
                }
            }

            if (queryAttribute.desiredContent != null) {
                if (queryAttribute.desiredContent == "Nachfolger")
                    queryAttributeData.contentOperator = ContentOperatorForUI.SHOW_SUCCESSORS
                else {
                    when (queryAttributeData.contentOperator) {
                        ContentOperatorForUI.EQUALS,
                        ContentOperatorForUI.LESS, ContentOperatorForUI.LESS_OR_EQUAL,
                        ContentOperatorForUI.MORE, ContentOperatorForUI.MORE_OR_EQUAL,
                        ContentOperatorForUI.CONTAINS, ContentOperatorForUI.CONTAINS_NOT,
                        ContentOperatorForUI.CONTAINS_POSITIVE, ContentOperatorForUI.CONTAINS_NOT_POSITIVE -> {
                            if (catalogEntry.dataType == CatalogEntryType.Number)
                                queryAttributeData.desiredContentNumeric = queryAttribute.desiredContentDouble
                            else if (catalogEntry.dataType == CatalogEntryType.DateTime)
                                queryAttributeData.desiredContentDate =
                                    java.sql.Date(queryAttribute.desiredContentDate.time).toLocalDate()
                            else
                                queryAttributeData.desiredContent = queryAttribute.desiredContent
                        }

                        ContentOperatorForUI.BETWEEN -> {
                            when (catalogEntry.dataType) {
                                CatalogEntryType.Number -> {
                                    queryAttributeData.desiredContentBetweenLowerBoundNumeric =
                                        queryAttribute.desiredContentBetweenLowerBoundDouble
                                    queryAttributeData.desiredContentBetweenUpperBoundNumeric =
                                        queryAttribute.desiredContentBetweenUpperBoundDouble
                                }

                                CatalogEntryType.DateTime -> {
                                    queryAttributeData.desiredContentBetweenLowerBoundDate =
                                        java.sql.Date(queryAttribute.desiredContentBetweenLowerBoundDate.time)
                                            .toLocalDate()
                                    queryAttributeData.desiredContentBetweenUpperBoundDate =
                                        java.sql.Date(queryAttribute.desiredContentBetweenUpperBoundDate.time)
                                            .toLocalDate()
                                }

                                else -> throw IllegalStateException(
                                    "The data type (${catalogEntry.dataType}) " +
                                            "of the catalog entry is not applicable for the content operator BETWEEN."
                                )
                            }
                        }

                        ContentOperatorForUI.PER_YEAR -> {
                            queryAttributeData.desiredContentBetweenLowerBoundYear =
                                queryAttribute.desiredContentBetweenLowerBoundDouble.toInt()
                            queryAttributeData.desiredContentBetweenUpperBoundYear =
                                queryAttribute.desiredContentBetweenUpperBoundDouble.toInt()
                        }

                        else -> {
                            // do nothing, because a desired content is not applicable for the other content operators
                        }
                    }
                }
            }

            if (queryAttribute.tempOpsAbs != null && queryAttribute.tempOpsAbs.isNotEmpty()) {
                val tempOpAbs = queryAttribute.tempOpsAbs.first()
                queryAttributeData.useTempOpAbsMin = tempOpAbs.absMinDate != null
                queryAttributeData.tempOpAbsMin = tempOpAbs.absMinDate?.toLocalDateTime()?.toLocalDate()
                queryAttributeData.useTempOpAbsMax = tempOpAbs.absMaxDate != null
                queryAttributeData.tempOpAbsMax = tempOpAbs.absMaxDate?.toLocalDateTime()?.toLocalDate()
            }

            return QueryAttributeContainer(
                catalogEntry = catalogEntry,
                queryAttributeData = queryAttributeData
            )
        }
    }

    fun getButtonTitle(): String {
        return when (connectionType) {
            QueryAttributeConnectionType.OR -> UI.getCurrent().getTranslation("web-padawan.queryAttributeButton.or")
            QueryAttributeConnectionType.AND -> UI.getCurrent().getTranslation("web-padawan.queryAttributeButton.and")
            QueryAttributeConnectionType.NONE -> {
                var title = catalogEntry!!.name

                val desiredContentToUse = getDesiredContentToUse()
                title = when (queryAttributeData.contentOperator) {
                    ContentOperatorForUI.NOT_EXISTS -> "! $title"
                    ContentOperatorForUI.SHOW_SUCCESSORS -> "${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.showSuccessors.printVariant")
                    } $title"

                    ContentOperatorForUI.EQUALS -> "$title = ${desiredContentToUse.first}"
                    ContentOperatorForUI.LESS -> "$title < ${desiredContentToUse.first}"
                    ContentOperatorForUI.LESS_OR_EQUAL -> "$title <= ${desiredContentToUse.first}"
                    ContentOperatorForUI.MORE -> "$title > ${desiredContentToUse.first}"
                    ContentOperatorForUI.MORE_OR_EQUAL -> "$title >= ${desiredContentToUse.first}"
                    ContentOperatorForUI.BETWEEN -> "$title ${
                        UI.getCurrent().getTranslation(
                            "web-padawan.queryAttributeDialog.restriction.attribute.between.printVariant",
                            desiredContentToUse.first,
                            desiredContentToUse.second
                        )
                    }"

                    ContentOperatorForUI.CONTAINS -> "$title ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.contains")
                    } '${desiredContentToUse.first}'"

                    ContentOperatorForUI.CONTAINS_NOT -> "$title ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.containsNot")
                    } '${desiredContentToUse.first}'"

                    ContentOperatorForUI.CONTAINS_POSITIVE -> "$title ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.containsPositive.printVariant")
                    } '${desiredContentToUse.first}'"

                    ContentOperatorForUI.CONTAINS_NOT_POSITIVE -> "$title ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.attribute.containsNotPositive")
                    } '${desiredContentToUse.first}'"

                    ContentOperatorForUI.PER_YEAR -> "$title: ${
                        UI.getCurrent().getTranslation(
                            "web-padawan.queryAttributeDialog.restriction.attribute.perYear.printVariant",
                            queryAttributeData.desiredContentBetweenLowerBoundYear!!.toString(),
                            queryAttributeData.desiredContentBetweenUpperBoundYear!!.toString()
                        )
                    }"

                    else -> title
                }

                title += when (queryAttributeData.reductionOperator) {
                    ReductionOperator.EARLIEST -> " (${
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator")
                    } ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.earliest")
                    })"

                    ReductionOperator.LATEST -> " (${
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator")
                    } ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.latest")
                    })"

                    ReductionOperator.MIN -> " (${
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator")
                    } ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.min")
                    })"

                    ReductionOperator.MAX -> " (${
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator")
                    } ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.reductionOperator.max")
                    })"

                    else -> ""
                }

                title += when (queryAttributeData.extractionMode) {
                    ExtractionMode.NextNumber -> " (${
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode")
                    } ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode.nextNumber")
                    })"

                    ExtractionMode.BetweenHits -> " (${
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode")
                    } ${
                        UI.getCurrent()
                            .getTranslation("web-padawan.queryAttributeDialog.restriction.extractionMode.betweenHits")
                    })"

                    ExtractionMode.None -> ""
                }

                if (queryAttributeData.useTempOpAbsMin || queryAttributeData.useTempOpAbsMax) {
                    title += " ("
                    val measureTimeString =
                        UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.measureTime")
                    title += if (!queryAttributeData.useTempOpAbsMin)
                        "$measureTimeString <= ${dateFormatter.format(queryAttributeData.tempOpAbsMax!!)}"
                    else if (!queryAttributeData.useTempOpAbsMax)
                        "$measureTimeString >= ${dateFormatter.format(queryAttributeData.tempOpAbsMin!!)}"
                    else
                        "${dateFormatter.format(queryAttributeData.tempOpAbsMin!!)} <= $measureTimeString <= " +
                                dateFormatter.format(queryAttributeData.tempOpAbsMax!!)
                    title += ")"
                }

                if (queryAttributeData.useForCorrelationAnalysis) title += " (${
                    UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.correlation.usedAttribute")
                })"

                if (queryAttributeData.optional) title += " (${
                    UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.restriction.optional").lowercase()
                })"

                if (!queryAttributeData.displayValue ||
                    queryAttributeData.onlyDisplayExistence ||
                    queryAttributeData.multipleRows ||
                    queryAttributeData.showCaseID ||
                    queryAttributeData.showDocID ||
                    queryAttributeData.showMeasureTime
                ) {
                    title += " (${UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.show")}: "
                    if (!queryAttributeData.displayValue)
                        title += "${
                            UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.show.value.printNot")
                        }, "
                    if (queryAttributeData.onlyDisplayExistence)
                        title += "${
                            UI.getCurrent()
                                .getTranslation("web-padawan.queryAttributeDialog.show.onlyExistence.printVariant")
                        }, "
                    if (queryAttributeData.multipleRows)
                        title += "${
                            UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.show.valuesInSeparateRows")
                        }, "
                    if (queryAttributeData.showCaseID)
                        title += "${
                            UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.show.caseID")
                        }, "
                    if (queryAttributeData.showDocID)
                        title += "${
                            UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.show.documentID")
                        }, "
                    if (queryAttributeData.showMeasureTime)
                        title += "${
                            UI.getCurrent().getTranslation("web-padawan.queryAttributeDialog.show.measureTime")
                        }, "
                    title = title.substring(0, title.length - 2)
                    title += " )"
                }

                title
            }
        }
    }

    private fun getDesiredContentToUse(): Pair<String, String> {
        return if (
            catalogEntry!!.dataType == CatalogEntryType.SingleChoice ||
            catalogEntry.dataType == CatalogEntryType.Text
        )
            Pair(queryAttributeData.desiredContent.orEmpty(), "")
        else if (catalogEntry.dataType == CatalogEntryType.Number) {
            if (queryAttributeData.contentOperator == ContentOperatorForUI.BETWEEN)
                Pair(
                    queryAttributeData.desiredContentBetweenLowerBoundNumeric!!.toString(),
                    queryAttributeData.desiredContentBetweenUpperBoundNumeric!!.toString()
                )
            else
                if (queryAttributeData.desiredContentNumeric == null)
                    Pair("", "")
                else
                    Pair(queryAttributeData.desiredContentNumeric!!.toString(), "")
        } else if (catalogEntry.dataType == CatalogEntryType.DateTime) {
            if (queryAttributeData.contentOperator == ContentOperatorForUI.BETWEEN)
                Pair(
                    dateFormatter.format(queryAttributeData.desiredContentBetweenLowerBoundDate!!),
                    dateFormatter.format(queryAttributeData.desiredContentBetweenUpperBoundDate!!)
                )
            else
                if (queryAttributeData.desiredContentDate == null)
                    Pair("", "")
                else
                    Pair(dateFormatter.format(queryAttributeData.desiredContentDate!!), "")
        } else
            Pair("", "")
    }

    fun addMXQLAttribute(parent: QueryStructureContainingElem, displayName: String? = null) {
        if (connectionType == QueryAttributeConnectionType.NONE) {
            val attribute = QueryAttribute(parent, catalogEntry!!)

            if (displayName != null) attribute.displayName = displayName

            attribute.setDisplayCaseID(queryAttributeData.showCaseID)
            attribute.setDisplayDocID(queryAttributeData.showDocID)
            attribute.setDisplayInfoDate(queryAttributeData.showMeasureTime)

            attribute.isOptional = queryAttributeData.optional
            attribute.setDisplayValue(queryAttributeData.displayValue)
            attribute.isOnlyDisplayExistence = queryAttributeData.onlyDisplayExistence
            attribute.isMultipleRows = queryAttributeData.multipleRows

            if (queryAttributeData.contentOperator == ContentOperatorForUI.SHOW_SUCCESSORS)
                attribute.desiredContent = "Nachfolger"
            else {
                attribute.contentOperator = when (queryAttributeData.contentOperator) {
                    ContentOperatorForUI.EMPTY -> ContentOperator.EMPTY
                    ContentOperatorForUI.EQUALS -> ContentOperator.EQUALS
                    ContentOperatorForUI.LESS -> ContentOperator.LESS
                    ContentOperatorForUI.LESS_OR_EQUAL -> ContentOperator.LESS_OR_EQUAL
                    ContentOperatorForUI.MORE -> ContentOperator.MORE
                    ContentOperatorForUI.MORE_OR_EQUAL -> ContentOperator.MORE_OR_EQUAL
                    ContentOperatorForUI.BETWEEN -> ContentOperator.BETWEEN
                    ContentOperatorForUI.CONTAINS -> ContentOperator.CONTAINS
                    ContentOperatorForUI.CONTAINS_NOT -> ContentOperator.CONTAINS_NOT
                    ContentOperatorForUI.CONTAINS_POSITIVE -> ContentOperator.CONTAINS_POSITIVE
                    ContentOperatorForUI.CONTAINS_NOT_POSITIVE -> ContentOperator.CONTAINS_NOT_POSITIVE
                    ContentOperatorForUI.PER_YEAR -> ContentOperator.PER_YEAR
                    ContentOperatorForUI.PER_MONTH -> ContentOperator.PER_MONTH
                    ContentOperatorForUI.PER_INTERVALS -> ContentOperator.PER_INTERVALS
                    ContentOperatorForUI.NOT_EXISTS -> ContentOperator.NOT_EXISTS
                    ContentOperatorForUI.EXISTS -> ContentOperator.EXISTS
                    else -> throw IllegalStateException("The content operator has an unexpected value: $queryAttributeData.contentOperator")
                }

                when (queryAttributeData.contentOperator) {
                    ContentOperatorForUI.EQUALS,
                    ContentOperatorForUI.LESS, ContentOperatorForUI.LESS_OR_EQUAL,
                    ContentOperatorForUI.MORE, ContentOperatorForUI.MORE_OR_EQUAL,
                    ContentOperatorForUI.CONTAINS, ContentOperatorForUI.CONTAINS_NOT,
                    ContentOperatorForUI.CONTAINS_POSITIVE, ContentOperatorForUI.CONTAINS_NOT_POSITIVE -> {
                        if (catalogEntry.dataType == CatalogEntryType.Number)
                            attribute.setDesiredContent(queryAttributeData.desiredContentNumeric!!)
                        else if (catalogEntry.dataType == CatalogEntryType.DateTime)
                            attribute.setDesiredContent(Timestamp.valueOf(queryAttributeData.desiredContentDate!!.atStartOfDay()))
                        else
                            attribute.desiredContent = queryAttributeData.desiredContent!!
                    }

                    ContentOperatorForUI.BETWEEN -> {
                        when (catalogEntry.dataType) {
                            CatalogEntryType.Number -> attribute.setDesiredContent(
                                queryAttributeData.desiredContentBetweenLowerBoundNumeric!!,
                                queryAttributeData.desiredContentBetweenUpperBoundNumeric!!
                            )

                            CatalogEntryType.DateTime -> attribute.setDesiredContent(
                                Timestamp.valueOf(queryAttributeData.desiredContentBetweenLowerBoundDate!!.atStartOfDay()),
                                Timestamp.valueOf(queryAttributeData.desiredContentBetweenUpperBoundDate!!.atStartOfDay())
                            )

                            else -> throw IllegalStateException("The data type of the catalog entry (${catalogEntry.dataType}) is not applicable for the content operator BETWEEN.")
                        }
                    }

                    ContentOperatorForUI.PER_YEAR -> {
                        attribute.desiredContent = "${queryAttributeData.desiredContentBetweenLowerBoundYear!!}" +
                                "$PERIOD_DELIMITER${queryAttributeData.desiredContentBetweenUpperBoundYear!!}"
                    }

                    else -> {
                        // do nothing, because a desired content is not applicable for the other content operators
                    }
                }
            }

            attribute.reductionOperator = queryAttributeData.reductionOperator
            attribute.extractionMode = queryAttributeData.extractionMode

            if (queryAttributeData.useTempOpAbsMin || queryAttributeData.useTempOpAbsMax) {
                val tempOpAbs = QueryTempOpAbs(attribute)
                if (queryAttributeData.useTempOpAbsMin)
                    tempOpAbs.absMinDate = Timestamp.valueOf(queryAttributeData.tempOpAbsMin!!.atStartOfDay())
                if (queryAttributeData.useTempOpAbsMax)
                    tempOpAbs.absMaxDate = Timestamp.valueOf(queryAttributeData.tempOpAbsMax!!.atStartOfDay())
                attribute.addTempOpAbs(tempOpAbs)
            }

            if (queryAttributeData.useForCorrelationAnalysis)
                attribute.comment =
                    PaDaWaNQueryConnector.CORRELATION_ANALYSIS_ATTRIBUTE_PREFIX + (attribute.comment ?: "")
        }
    }
}
