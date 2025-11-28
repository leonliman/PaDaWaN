package de.uniwue.misbased.vaadin_padawan.data.imageConnector

import de.uniwue.dw.core.client.api.configuration.DwClientConfiguration
import de.uniwue.dw.core.client.authentication.User
import de.uniwue.dw.core.model.data.CatalogEntry
import de.uniwue.dw.query.model.lang.DisplayStringVisitor
import de.uniwue.dw.query.model.lang.QueryAttribute
import de.uniwue.dw.query.model.lang.QueryRoot
import de.uniwue.dw.query.model.result.Result
import de.uniwue.misbased.vaadin_padawan.data.PARAM_IMAGE_CATALOG_ENTRY_EXTERNAL_ID
import de.uniwue.misbased.vaadin_padawan.data.PARAM_IMAGE_CATALOG_ENTRY_PROJECT
import de.uniwue.misbased.vaadin_padawan.data.PARAM_IMAGE_DOWNLOAD_IDS_TO_EXCLUDE_FILE
import de.uniwue.misbased.vaadin_padawan.data.PARAM_IMAGE_DOWNLOAD_ROOT_FOLDER
import de.uniwue.misbased.vaadin_padawan.data.PARAM_IMAGE_DOWNLOAD_ROOT_FOLDER_EXTERNAL_ACCESS_PATH
import de.uniwue.misbased.vaadin_padawan.data.PARAM_IMAGE_ROOT_FOLDER
import de.uniwue.misbased.vaadin_padawan.data.padawanConnector.PaDaWaNConnector
import org.apache.commons.io.FilenameUtils
import java.io.File
import java.util.concurrent.ThreadLocalRandom

object ImageConnector {

    private var imageConnectionWorking: Boolean? = null
    private var imageRootFolder: File? = null
    private var imageCatalogEntry: CatalogEntry? = null
    private var downloadRootFolder: File? = null
    private var downloadRootFolderExternalAccessPath: String? = null

    fun isImageConnectionWorking(): Boolean {
        if (imageConnectionWorking == null)
            imageConnectionWorking = getImageRootFolder() != null && getImageCatalogEntry() != null &&
                    getDownloadRootFolder() != null && getDownloadRootFolderExternalAccessPath() != null
        return imageConnectionWorking!!
    }

    private fun getImageRootFolder(): File? {
        if (imageConnectionWorking == null) {
            val rootFolderPath = DwClientConfiguration.getInstance().getParameter(PARAM_IMAGE_ROOT_FOLDER)
            if (rootFolderPath == null)
                return null
            val rootFolder = File(rootFolderPath)
            if (!rootFolder.exists() || !rootFolder.isDirectory || !rootFolder.canRead())
                return null
            imageRootFolder = rootFolder
        }
        return imageRootFolder
    }

    private fun getImageCatalogEntry(): CatalogEntry? {
        if (imageConnectionWorking == null) {
            val project = DwClientConfiguration.getInstance().getParameter(PARAM_IMAGE_CATALOG_ENTRY_PROJECT)
            val extID = DwClientConfiguration.getInstance().getParameter(PARAM_IMAGE_CATALOG_ENTRY_EXTERNAL_ID)
            if (project == null || extID == null)
                return null
            imageCatalogEntry = try {
                PaDaWaNConnector.getCatalogManager().getEntryByRefID(extID, project, false)
            } catch (e: Exception) {
                e.printStackTrace()
                null
            }
        }
        return imageCatalogEntry
    }

    private fun getDownloadRootFolder(): File? {
        if (imageConnectionWorking == null) {
            val downloadRootFolderPath =
                DwClientConfiguration.getInstance().getParameter(PARAM_IMAGE_DOWNLOAD_ROOT_FOLDER)
            if (downloadRootFolderPath == null)
                return null
            val rootFolder = File(downloadRootFolderPath)
            if (!rootFolder.exists() || !rootFolder.isDirectory || !rootFolder.canWrite())
                return null
            downloadRootFolder = rootFolder
        }
        return downloadRootFolder
    }

    private fun getDownloadRootFolderExternalAccessPath(): String? {
        if (imageConnectionWorking == null)
            downloadRootFolderExternalAccessPath =
                DwClientConfiguration.getInstance().getParameter(PARAM_IMAGE_DOWNLOAD_ROOT_FOLDER_EXTERNAL_ACCESS_PATH)
        return downloadRootFolderExternalAccessPath
    }

    fun getImageColumnHeader() = QueryAttribute(QueryRoot(), getImageCatalogEntry()).accept(DisplayStringVisitor())!!

    fun getImageFilesForImageIDs(imageIDs: List<String>): Map<String, File> {
        val rootFolder = getImageRootFolder() ?: return emptyMap()
        return rootFolder.listFiles { file ->
            val baseName = FilenameUtils.getBaseName(file.name)
            file.isFile && file.canRead() && imageIDs.contains(baseName)
        }?.associateBy({ FilenameUtils.getBaseName(it.name) }, { it }) ?: emptyMap()
    }

    fun getImageIDsForCompleteDownload(user: User, query: QueryRoot): Pair<Boolean, Any> {
        query.limitResult = 0

        val queryRunner = PaDaWaNConnector.getGUIClient().queryRunner
        val queryID = queryRunner.createQuery(query, user)
        val result = queryRunner.runQueryBlocking(queryID)

        if (!resultContainsImageColumn(result))
            return Pair(false, "web-padawan.images.error.noImageIDEntry")

        val imageIDs = mutableListOf<String>()
        val imageColumnHeader = getImageColumnHeader()
        for (aRow in result.rows)
            for (aCell in aRow.cells)
                if (aCell.columnName == imageColumnHeader)
                    imageIDs.addAll(aCell.valueAsString.split(" | "))

        return Pair(true, ImageIDsForDownload(imageIDs))
    }

    data class ImageIDsForDownload(val imageIDs: List<String>)

    fun resultContainsImageColumn(result: Result): Boolean {
        if (!isImageConnectionWorking())
            return false
        val imageColumnHeader = getImageColumnHeader()
        for (aHeader in result.header)
            if (aHeader == imageColumnHeader)
                return true
        return false
    }

    fun performImagesDownload(imageIDs: List<String>): Pair<Boolean, String?> {
        return try {
            val imageFiles = getImageFilesForImageIDs(imageIDs)
            if (imageFiles.isEmpty())
                return Pair(false, "No matching picture(s) could be found")

            val rootFolder = getDownloadRootFolder()!!
            var targetFolder = File(rootFolder, ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE).toString())
            while (targetFolder.exists())
                targetFolder = File(rootFolder, ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE).toString())
            targetFolder.mkdir()

            imageFiles.values.forEach { imageFile ->
                val targetFile = File(targetFolder, imageFile.name)
                imageFile.copyTo(targetFile, true)
            }

            val externalReturnPath = "${getDownloadRootFolderExternalAccessPath()!!}\\${targetFolder.name}"
            Pair(true, externalReturnPath)
        } catch (e: Exception) {
            e.printStackTrace()
            Pair(false, e.message)
        }
    }

    fun getDownloadIDsToExclude(): List<String> {
        val resultList = mutableListOf<String>()
        val idsToExcludeFilePath = DwClientConfiguration.getInstance().getParameter(
            PARAM_IMAGE_DOWNLOAD_IDS_TO_EXCLUDE_FILE
        )
        if (idsToExcludeFilePath == null)
            return resultList
        val idsToExcludeFile = File(idsToExcludeFilePath)
        if (!idsToExcludeFile.exists() || !idsToExcludeFile.isFile || !idsToExcludeFile.canRead())
            return resultList
        idsToExcludeFile.forEachLine { resultList.add(it) }
        return resultList
    }
}