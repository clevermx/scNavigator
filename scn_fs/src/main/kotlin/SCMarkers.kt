package ru.itmo.scn.fs
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.File
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
// Data classes
@Serializable
data class MarkerEntry(
    @SerialName("p_val")
    val pValue: Double,
    @SerialName("p_val_adj")
    val pValueAdjusted: Double,
    @SerialName("avg_logFC")
    val averageLogFoldChange: Double,
    @SerialName("pct.1")
    val pct1: Double,
    @SerialName("pct.2")
    val pct2: Double,
    @SerialName("cluster")
    val cluster: String,
    @SerialName("gene")
    val gene: String
)
@Serializable
data class SCMarkerEntry(
    val _id: org.litote.kmongo.Id<SCMarkerEntry> = org.litote.kmongo.newId(),
    val token: String,
    val tableName: String,
    val pValue: Double,
    val pValueAdjusted: Double,
    val averageLogFoldChange: Double,
    val pct1: Double,
    val pct2: Double,
    val cluster: String,
    val gene: String
)
// Collection class
@Serializable
data class MarkerCollection(
    val collection: Map<String, List<MarkerEntry>>
) {
    companion object Factory {
        // Setup ObjectMapper
        private val objectMapper: ObjectMapper = ObjectMapper(JsonFactory().apply {
            enable(JsonParser.Feature.ALLOW_COMMENTS)
            enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
            enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
        })
        fun flowFromJsonFile(filePath: String): Flow<Pair<String, MarkerEntry>> = flow {
            val jsonFactory = JsonFactory()
            val jsonParser = jsonFactory.createParser(File(filePath))
            objectMapper.factory = jsonFactory // Attach ObjectMapper
            jsonParser.use {
                while (jsonParser.nextToken() != com.fasterxml.jackson.core.JsonToken.END_ARRAY) {
                    val entryNode: JsonNode = objectMapper.readTree(jsonParser)
                    val tableName = entryNode.get("key").asText()
                    entryNode.get("value").forEach { itemNode ->
                        val markerEntry = MarkerEntry(
                            pValue = itemNode.get("p_val").asDouble(),
                            pValueAdjusted = itemNode.get("p_val_adj").asDouble(),
                            averageLogFoldChange = itemNode.get("avg_logFC").asDouble(),
                            pct1 = itemNode.get("pct.1").asDouble(),
                            pct2 = itemNode.get("pct.2").asDouble(),
                            cluster = itemNode.get("cluster").asText(),
                            gene = itemNode.get("gene").asText()
                        )
                        emit(tableName to markerEntry) // Emit each entry pair
                    }
                }
            }
        }
    }
}
