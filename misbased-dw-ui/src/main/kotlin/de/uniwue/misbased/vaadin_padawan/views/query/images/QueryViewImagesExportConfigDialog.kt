package de.uniwue.misbased.vaadin_padawan.views.query.images

import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.html.Span
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.HorizontalLayout
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.component.radiobutton.RadioButtonGroup
import com.vaadin.flow.component.radiobutton.RadioGroupVariant
import com.vaadin.flow.component.textfield.IntegerField
import com.vaadin.flow.data.binder.Binder
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.views.query.QueryAttributesCustomField

class QueryViewImagesExportConfigDialog(
    imageIDs: List<String>,
    idsToExclude: List<String>,
    private val onCancel: () -> Unit,
    private val onDownload: (List<String>) -> Unit
) : Dialog() {

    init {
        isCloseOnEsc = false
        isCloseOnOutsideClick = false

        headerTitle = getTranslation("web-padawan.images.download.configDialog.title")

        val imageIDsToUse = imageIDs.toSet().toMutableList()
        var numExcludedIDs = 0
        for (id in idsToExclude.toSet())
            if (imageIDsToUse.remove(id))
                numExcludedIDs++
        var imageCountText = if (idsToExclude.isEmpty())
            imageIDsToUse.size.toString()
        else
            "(${imageIDsToUse.size + numExcludedIDs} - $numExcludedIDs =) ${imageIDsToUse.size}"

        val exportConfigBinder = Binder<ImageExportConfig>()
        exportConfigBinder.bean = ImageExportConfig(imageIDsToUse, amount = imageIDsToUse.size)

        val contentLayout = createContentLayout(imageCountText, exportConfigBinder)
        contentLayout.setWidthFull()
        add(contentLayout)

        val bottomButtonsLayout = createBottomButtonsLayout(exportConfigBinder)
        bottomButtonsLayout.setWidthFull()
        footer.add(bottomButtonsLayout)
    }

    private fun createContentLayout(
        imageCountText: String,
        exportConfigBinder: Binder<ImageExportConfig>
    ): VerticalLayout {
        val contentLayout = VerticalLayout()

        val mainContentText = Span()
        mainContentText.element.setProperty(
            "innerHTML",
            getTranslation("web-padawan.images.download.configDialog.mainContent", imageCountText)
        )
        mainContentText.setWidthFull()

        val downloadModeRadioButtonGroup = RadioButtonGroup<DownloadMode>()
        downloadModeRadioButtonGroup.addThemeVariants(RadioGroupVariant.LUMO_VERTICAL)
        downloadModeRadioButtonGroup.setItems(DownloadMode.entries)
        downloadModeRadioButtonGroup.setItemLabelGenerator { downloadMode ->
            when (downloadMode) {
                DownloadMode.COMPLETE -> getTranslation("web-padawan.images.download.configDialog.downloadMode.complete")
                DownloadMode.PARTIAL -> getTranslation("web-padawan.images.download.configDialog.downloadMode.partial")
            }
        }
        downloadModeRadioButtonGroup.setWidthFull()
        if (exportConfigBinder.bean.imageIDsToExport.size < 2)
            downloadModeRadioButtonGroup.isReadOnly = true
        exportConfigBinder.forField(downloadModeRadioButtonGroup)
            .bind(ImageExportConfig::downloadMode::get, ImageExportConfig::downloadMode::set)

        val startPositionIntegerField = IntegerField()
        startPositionIntegerField.label = getTranslation("web-padawan.images.download.configDialog.startPosition")
        startPositionIntegerField.min = 1
        startPositionIntegerField.max = exportConfigBinder.bean.imageIDsToExport.size
        startPositionIntegerField.isStepButtonsVisible = true
        startPositionIntegerField.setWidthFull()
        startPositionIntegerField.isVisible = false
        exportConfigBinder.forField(startPositionIntegerField)
            .asRequired(getTranslation("web-padawan.images.download.configDialog.startPosition.error"))
            .bind(ImageExportConfig::startPosition::get, ImageExportConfig::startPosition::set)

        val amountIntegerField = IntegerField()
        amountIntegerField.label = getTranslation("web-padawan.images.download.configDialog.amount")
        amountIntegerField.min = 1
        amountIntegerField.max = exportConfigBinder.bean.imageIDsToExport.size
        amountIntegerField.isStepButtonsVisible = true
        amountIntegerField.setWidthFull()
        amountIntegerField.isVisible = false
        exportConfigBinder.forField(amountIntegerField)
            .asRequired(getTranslation("web-padawan.images.download.configDialog.amount.error"))
            .bind(ImageExportConfig::amount::get, ImageExportConfig::amount::set)

        downloadModeRadioButtonGroup.addValueChangeListener {
            startPositionIntegerField.isVisible = it.value == DownloadMode.PARTIAL
            amountIntegerField.isVisible = it.value == DownloadMode.PARTIAL
            QueryAttributesCustomField.forceUIResize()
        }

        contentLayout.add(mainContentText, downloadModeRadioButtonGroup, startPositionIntegerField, amountIntegerField)
        return contentLayout
    }

    private data class ImageExportConfig(
        var imageIDsToExport: List<String> = emptyList(),
        var downloadMode: DownloadMode = DownloadMode.COMPLETE,
        var startPosition: Int = 1,
        var amount: Int = 1
    ) {
        fun getIDsToExport(): List<String> {
            val sortedImageIDsToExport = imageIDsToExport.sorted()
            return if (downloadMode == DownloadMode.COMPLETE)
                sortedImageIDsToExport
            else {
                if (startPosition + amount - 1 > sortedImageIDsToExport.size)
                    sortedImageIDsToExport.subList(startPosition - 1, sortedImageIDsToExport.size)
                else
                    sortedImageIDsToExport.subList(startPosition - 1, startPosition + amount - 1)
            }
        }
    }

    private enum class DownloadMode {
        COMPLETE, PARTIAL
    }

    private fun createBottomButtonsLayout(exportConfigBinder: Binder<ImageExportConfig>): HorizontalLayout {
        val bottomButtonsLayout = HorizontalLayout()

        val cancelButton = createCancelButton()
        val startDownloadButton = createStartDownloadButton(exportConfigBinder)

        bottomButtonsLayout.addAndExpand(cancelButton, startDownloadButton)
        return bottomButtonsLayout
    }

    private fun createCancelButton(): Button {
        val cancelIcon = FontIcon(FONT_ICON_FAMILY, "fa-ban")
        val cancelButton = Button(getTranslation("web-padawan.settings.cancel"), cancelIcon)
        cancelButton.addClickListener {
            close()
            onCancel()
        }
        return cancelButton
    }

    private fun createStartDownloadButton(exportConfigBinder: Binder<ImageExportConfig>): Button {
        val saveIcon = FontIcon(FONT_ICON_FAMILY, "fa-check")
        val startDownloadButton = Button(getTranslation("web-padawan.images.buttons.downloadSinglePicture"), saveIcon)
        startDownloadButton.addClickListener {
            if (exportConfigBinder.validate().isOk) {
                close()
                onDownload(exportConfigBinder.bean.getIDsToExport())
            }
        }
        if (exportConfigBinder.bean.imageIDsToExport.isEmpty())
            startDownloadButton.isEnabled = false
        return startDownloadButton
    }
}