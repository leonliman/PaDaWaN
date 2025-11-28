package de.uniwue.misbased.vaadin_padawan.views.result

import com.vaadin.flow.component.Text
import com.vaadin.flow.component.button.Button
import com.vaadin.flow.component.dialog.Dialog
import com.vaadin.flow.component.html.Image
import com.vaadin.flow.component.icon.FontIcon
import com.vaadin.flow.component.orderedlayout.FlexComponent
import com.vaadin.flow.component.orderedlayout.Scroller
import com.vaadin.flow.component.orderedlayout.VerticalLayout
import com.vaadin.flow.server.StreamResource
import de.uniwue.misbased.vaadin_padawan.data.FONT_ICON_FAMILY
import de.uniwue.misbased.vaadin_padawan.data.imageConnector.ImageConnector

class ImagesDialog(imageIDs: List<String>) : Dialog() {

    init {
        width = "90%"
        height = "90%"

        headerTitle = getTranslation("web-padawan.images.dialog.title", imageIDs.size)
        header.add(createCloseButton())

        add(createContent(imageIDs))
    }

    private fun createCloseButton(): Button {
        val closeIcon = FontIcon(FONT_ICON_FAMILY, "fa-times")
        return Button(closeIcon) {
            close()
        }
    }

    private fun createContent(imageIDs: List<String>): Scroller {
        val scroller = Scroller()
        scroller.setWidthFull()
        scroller.scrollDirection = Scroller.ScrollDirection.VERTICAL
        val imageFiles = ImageConnector.getImageFilesForImageIDs(imageIDs)
        if (imageFiles.isEmpty())
            scroller.content = Text(getTranslation("web-padawan.images.dialog.noImages"))
        else {
            val imagesContainer = VerticalLayout()
            imagesContainer.setWidthFull()
            imagesContainer.alignItems = FlexComponent.Alignment.CENTER
            imageIDs.forEach { imageID ->
                val imageFile = imageFiles[imageID]
                if (imageFile != null) {
                    val streamResource = StreamResource(imageFile.name) {
                        imageFile.inputStream()
                    }
                    val image = Image(streamResource, imageID)
                    image.maxHeight = "70vh"
                    val imageSubTitle = Text("($imageID)")
                    imagesContainer.add(image, imageSubTitle)
                }
            }
            scroller.content = imagesContainer
        }
        return scroller
    }
}