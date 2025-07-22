package ru.itmo.scn.fs
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.File
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
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
@Serializable
data class MarkerCollection(
    val collection: Map<String, List<MarkerEntry>>
) {
    companion object Factory {
        private val objectMapper: ObjectMapper = ObjectMapper(JsonFactory().apply {
            enable(JsonParser.Feature.ALLOW_COMMENTS)
            enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
            enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
        })
        fun flowFromJsonFile(filePath: String): Flow<Pair<String, MarkerEntry>> = flow {
            val jsonParser = objectMapper.factory.createParser(File(filePath))
            jsonParser.use {
                while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                    val entryNode: JsonNode = objectMapper.readTree(jsonParser)
                    val tableName = entryNode.get("key").asText()
                    // Ensure itemNode is immutable
                    val itemsNode = entryNode.get("value")
                    itemsNode.forEach { itemNode ->
                        try {
                            val markerEntry = MarkerEntry(
                                pValue = itemNode.get("p_val")?.asDouble()
                                    ?: throw IllegalArgumentException("Missing or invalid p_val: ${itemNode.get("p_val")}"),
                                pValueAdjusted = itemNode.get("p_val_adj")?.asDouble()
                                    ?: throw IllegalArgumentException("Missing or invalid p_val_adj: ${itemNode.get("p_val_adj")}"),
                                averageLogFoldChange = itemNode.get("avg_logFC")?.asDouble()
                                    ?: throw IllegalArgumentException("Missing or invalid avg_logFC: ${itemNode.get("avg_logFC")}"),
                                pct1 = itemNode.get("pct.1")?.asDouble()
                                    ?: throw IllegalArgumentException("Missing or invalid pct.1: ${itemNode.get("pct.1")}"),
                                pct2 = itemNode.get("pct.2")?.asDouble()
                                    ?: throw IllegalArgumentException("Missing or invalid pct.2: ${itemNode.get("pct.2")}"),
                                cluster = itemNode.get("cluster")?.asText()
                                    ?: throw IllegalArgumentException("Missing or invalid cluster: ${itemNode.get("cluster")}"),
                                gene = itemNode.get("gene")?.asText()
                                    ?: throw IllegalArgumentException("Missing or invalid gene: ${itemNode.get("gene")}")
                            )
                            emit(tableName to markerEntry) // Emit each entry pair
                        } catch (e: IllegalArgumentException) {
                            println("Error parsing MarkerEntry: ${e.message}")
                        }
                    }
                }
            }
        }
    }
}