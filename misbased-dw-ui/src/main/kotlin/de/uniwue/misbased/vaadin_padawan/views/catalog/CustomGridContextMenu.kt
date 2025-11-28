package de.uniwue.misbased.vaadin_padawan.views.catalog

import com.vaadin.flow.component.grid.contextmenu.GridContextMenu
import elemental.json.JsonObject

open class CustomGridContextMenu<T> : GridContextMenu<T>() {
    override fun onBeforeOpenMenu(eventDetail: JsonObject?): Boolean {
        if (eventDetail != null) {
            val key = eventDetail.getString("key")
            if (key == null || key.isBlank())
                return false
        }
        return super.onBeforeOpenMenu(eventDetail)
    }
}