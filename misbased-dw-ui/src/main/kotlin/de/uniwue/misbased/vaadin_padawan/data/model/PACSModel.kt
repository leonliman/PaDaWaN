package de.uniwue.misbased.vaadin_padawan.data.model

enum class PACSStatus {
    CONFIGURATION_ERROR, OFFLINE, ONLINE
}

enum class PACSExportType {
    DCM, JPG
}

data class PACSModality(val shortText: String) {
    val longTextTranslationKey: String
        get() {
            return "web-padawan.pacs.modality.${shortText.lowercase()}"
        }

}

data class PACSUserSettings(
    val allModalities: List<PACSModality>,
    var enabledModalities: MutableSet<PACSModality>,
    var exportType: PACSExportType = PACSExportType.DCM
) {

    fun getExportTypeURLAppendix(): String {
        return "&imagetype=${exportType.name}"
    }

    fun getModalitiesURLAppendix(): String {
        var result = ""
        for (aModality in enabledModalities) {
            if (result.isEmpty())
                result = "&modality="
            else
                result += ","
            result += aModality.shortText
        }
        return result
    }
}