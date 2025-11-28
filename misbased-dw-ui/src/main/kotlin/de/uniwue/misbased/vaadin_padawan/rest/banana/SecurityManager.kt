package de.uniwue.misbased.vaadin_padawan.rest.banana

import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.core.model.manager.DataSourceException
import de.uniwue.dw.query.model.client.GUIClientException
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import java.io.IOException
import java.sql.SQLException

class SecurityManager {

    companion object {
        @Throws(DataSourceException::class, GUIClientException::class, IOException::class, SQLException::class)
        fun getCatalogEntry(attrID: Int, user: User): CatalogEntry {
            return PaDaWaNConnector.getGUIClient().catalogClientProvider.getEntryByID(attrID, user)
        }

        fun userIsAllowedToDoSelectQuery(body: String): Boolean {
            return body.contains("rows=0")
        }
    }
}