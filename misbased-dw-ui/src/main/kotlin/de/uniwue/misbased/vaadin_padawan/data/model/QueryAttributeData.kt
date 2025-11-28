package de.uniwue.misbased.vaadin_padawan.data.model

import de.uniwue.dw.query.model.lang.ExtractionMode
import de.uniwue.dw.query.model.lang.ReductionOperator
import java.time.LocalDate

data class QueryAttributeData(
    var showCaseID: Boolean = false,
    var showDocID: Boolean = false,
    var showMeasureTime: Boolean = false,
    var optional: Boolean = false,
    var displayValue: Boolean = true,
    var onlyDisplayExistence: Boolean = false,
    var multipleRows: Boolean = false,
    var contentOperator: ContentOperatorForUI = ContentOperatorForUI.EXISTS,
    var reductionOperator: ReductionOperator = ReductionOperator.NONE,
    var desiredContent: String? = null,
    var desiredContentNumeric: Double? = null,
    var desiredContentBetweenLowerBoundNumeric: Double? = null,
    var desiredContentBetweenUpperBoundNumeric: Double? = null,
    var desiredContentDate: LocalDate? = null,
    var desiredContentBetweenLowerBoundDate: LocalDate? = null,
    var desiredContentBetweenUpperBoundDate: LocalDate? = null,
    var desiredContentBetweenLowerBoundYear: Int? = null,
    var desiredContentBetweenUpperBoundYear: Int? = null,
    var extractionMode: ExtractionMode = ExtractionMode.None,
    var useTempOpAbsMin: Boolean = false,
    var tempOpAbsMin: LocalDate? = null,
    var useTempOpAbsMax: Boolean = false,
    var tempOpAbsMax: LocalDate? = null,
    var useForCorrelationAnalysis: Boolean = false
)

enum class ContentOperatorForUI {
    EMPTY,
    EQUALS,
    LESS,
    LESS_OR_EQUAL,
    MORE,
    MORE_OR_EQUAL,
    BETWEEN,
    CONTAINS,
    CONTAINS_NOT,
    CONTAINS_POSITIVE,
    CONTAINS_NOT_POSITIVE,
    PER_YEAR,
    PER_MONTH,
    PER_INTERVALS,
    NOT_EXISTS,
    EXISTS,
    SHOW_SUCCESSORS
}
