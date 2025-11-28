package de.uniwue.misbased.vaadin_padawan.views.query.attributePopup

import com.vaadin.flow.component.UI
import com.vaadin.flow.component.checkbox.Checkbox
import com.vaadin.flow.component.html.H4
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.data.binder.Binder
import com.vaadin.flow.data.binder.Setter
import com.vaadin.flow.function.ValueProvider
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeData

class QueryAttributePopupShowSection(binder: Binder<QueryAttributeData>) : VerticalLayout() {

    companion object {
        fun createAttributePopupCheckbox(
            binder: Binder<QueryAttributeData>,
            titleKey: String,
            getter: ValueProvider<QueryAttributeData, Boolean>,
            setter: Setter<QueryAttributeData, Boolean>
        ): Checkbox {
            val attributePopupCheckbox = Checkbox(UI.getCurrent().getTranslation(titleKey))

            binder.forField(attributePopupCheckbox)
                .bind(getter, setter)
            return attributePopupCheckbox
        }
    }

    init {
        isPadding = false
        isSpacing = false

        val sectionTitle = H4(getTranslation("web-padawan.queryAttributeDialog.show"))

        val showValueCheckbox = createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.show.value",
            QueryAttributeData::displayValue::get,
            QueryAttributeData::displayValue::set
        )
        showValueCheckbox.setWidthFull()
        val onlyExistenceCheckbox = createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.show.onlyExistence",
            QueryAttributeData::onlyDisplayExistence::get,
            QueryAttributeData::onlyDisplayExistence::set
        )
        onlyExistenceCheckbox.setWidthFull()
        add(sectionTitle, showValueCheckbox, onlyExistenceCheckbox)

        val valuesInSeparateRowsCheckbox = createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.show.valuesInSeparateRows",
            QueryAttributeData::multipleRows::get,
            QueryAttributeData::multipleRows::set
        )
        valuesInSeparateRowsCheckbox.setWidthFull()
        val caseIDCheckbox = createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.show.caseID",
            QueryAttributeData::showCaseID::get,
            QueryAttributeData::showCaseID::set
        )
        caseIDCheckbox.setWidthFull()
        val documentIDCheckbox = createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.show.documentID",
            QueryAttributeData::showDocID::get,
            QueryAttributeData::showDocID::set
        )
        documentIDCheckbox.setWidthFull()
        val measureTimeCheckbox = createAttributePopupCheckbox(
            binder,
            "web-padawan.queryAttributeDialog.show.measureTime",
            QueryAttributeData::showMeasureTime::get,
            QueryAttributeData::showMeasureTime::set
        )
        measureTimeCheckbox.setWidthFull()

        add(
            valuesInSeparateRowsCheckbox,
            caseIDCheckbox,
            documentIDCheckbox,
            measureTimeCheckbox
        )
    }
}