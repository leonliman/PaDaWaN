package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.Component
import com.vaadin.flow.component.customfield.CustomField
import com.vaadin.flow.component.html.Anchor
import com.vaadin.flow.component.orderedlayout.HorizontalLayout

class MultipleComponentsCustomField(components: List<Component>, downloadAnchor: Anchor? = null) :
    CustomField<String>() {

    init {
        val componentsContainerLayout = HorizontalLayout()
        componentsContainerLayout.add(components)
        downloadAnchor?.let { componentsContainerLayout.add(it) }

        add(componentsContainerLayout)
    }

    override fun generateModelValue(): String {
        return "" // not used
    }

    override fun setPresentationValue(newPresentationValue: String?) {
        // not used
    }
}