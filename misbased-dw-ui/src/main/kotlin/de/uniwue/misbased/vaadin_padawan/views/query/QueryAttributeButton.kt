package de.uniwue.misbased.vaadin_padawan.views.query

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.icon.FontIcon
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeConnectionType
import de.uniwue.misbased.vaadin_padawan.data.model.QueryAttributeContainer

class QueryAttributeButton(val queryAttributeContainer: QueryAttributeContainer) : Button() {

    init {
        text = queryAttributeContainer.getButtonTitle()
        if (queryAttributeContainer.connectionType == QueryAttributeConnectionType.NONE) {
            val selectableIndicatorIcon = FontIcon(FONT_ICON_FAMILY, "fa-caret-down")
            icon = selectableIndicatorIcon
            isIconAfterText = true
        }
    }
}