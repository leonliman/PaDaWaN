package de.uniwue.misbased.vaadin_padawan.views.result

import com.vaadin.flow.component.Html
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.Scroller
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY

class ResultFullTextDialog(columnHeader: String, fullText: String) : Dialog() {

    init {
        width = "80%"
        height = "80%"

        headerTitle = columnHeader
        header.add(createCloseButton())

        add(createContent(fullText))
    }

    private fun createCloseButton(): Button {
        val closeIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
        return Button(closeIcon) {
            close()
        }
    }

    private fun createContent(fullText: String): Scroller {
        val scroller = Scroller()
        scroller.setWidthFull()
        scroller.scrollDirection = Scroller.ScrollDirection.VERTICAL
        scroller.content = Html("<span>$fullText</span>")
        return scroller
    }
}