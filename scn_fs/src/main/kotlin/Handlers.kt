package ru.itmo.scn.fs

import com.mongodb.client.MongoCollection
import de.jupf.staticlog.Log
import kotlinx.coroutines.channels.Channel
import org.litote.kmongo.deleteOneById
import org.litote.kmongo.eq
import org.litote.kmongo.findOne
import org.litote.kmongo.updateOneById

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.withContext

import java.io.File

var max_exp_file = 0
fun insertSCDataset(path: Path,
                            mongoDBCollection: MongoCollection<SCDataset>,
                            mongoDBCollectionExp: MongoCollection<SCDatasetExpression>,
                            mongoDBCollectionMarkers: MongoCollection<SCMarkerEntry>) {

    try{
        val scDataset = SCDataset.fromJsonFile(path)

            Log.info("Inserting the dataset ${scDataset.token} into the mongo database")
            mongoDBCollection.insertOne(scDataset)

        if (scDataset.expressionFile !== null) {
            val cur_file_size = scDataset.expressionFile.length() / (1024.0 * 1024.0)
            if (cur_file_size > max_exp_file){
                max_exp_file = cur_file_size
            }
            val scExp = SCDatasetExpression.fromJsonFile(scDataset.expressionFile, scDataset.token)
            Log.info("Inserting the expression info for ${scExp.token} into the mongo database. cur_size:${cur_file_size}MB, max_size:${max_exp_file}MB")
            mongoDBCollectionExp.insertOne(scExp)   
        }

        if (scDataset.markersFile !== null) {
            Log.info("Updating the markers info for dataset ${scDataset.token} in the database")
            val entriesFlow = MarkerCollection.flowFromJsonFile(filePath)
            withContext(Dispatchers.IO) {
                log.info("Updating the markers info for dataset $token in the database")
                mongoDBCollectionMarkers.deleteMany(SCMarkerEntry::token eq scDataset.token) // Clear previous entries
                // Collect and process each entry
                entriesFlow.collect { (tableName, markerEntry) ->
                    val scMarkerEntry = SCMarkerEntry(
                        token = scDataset.token,
                        tableName = tableName,
                        cluster = markerEntry.cluster,
                        gene = markerEntry.gene,
                        pct1 = markerEntry.pct1,
                        pct2 = markerEntry.pct2,
                        pValue = markerEntry.pValue,
                        pValueAdjusted = markerEntry.pValueAdjusted,
                        averageLogFoldChange = markerEntry.averageLogFoldChange
                    )
                    mongoDBCollectionMarkers.insertOne(scMarkerEntry)
                }
            }
            Log.info("Updated markers for ${scDataset.token}")
        }
    } catch (e: Exception) {
        Log.error("Error while parsing $path. See exception text below")
        Log.error(e.message.toString())
    }                 

}

fun insertOrUpdateSCDataset(path: Path,
                            mongoDBCollection: MongoCollection<SCDataset>,
                            mongoDBCollectionExp: MongoCollection<SCDatasetExpression>,
                            mongoDBCollectionMarkers: MongoCollection<SCMarkerEntry>) {
    try {
        val scDataset = SCDataset.fromJsonFile(path)
        val datasetQuery = mongoDBCollection.findOne(SCDataset::token eq scDataset.token)
        var updated = false

        if (datasetQuery == null) {
            Log.info("Inserting the dataset ${scDataset.token} into the mongo database")
            mongoDBCollection.insertOne(scDataset)
            updated = true
        } else {
            if (datasetQuery.selfPath == scDataset.selfPath) {
                Log.info("Updating the dataset ${scDataset.token} in the database")
                mongoDBCollection.updateOneById(datasetQuery._id, scDataset)
                updated = true
            } else {
                Log.error("Dataset with token ${scDataset.token} already exists: ${datasetQuery.selfPath}. Not updating")
            }
        }

        if (updated) {
            if (scDataset.expressionFile !== null) {
                val cur_file_size = scDataset.expressionFile.length() / (1024.0 * 1024.0)
                if (cur_file_size > max_exp_file){
                    max_exp_file = cur_file_size
                }
                val scExp = SCDatasetExpression.fromJsonFile(scDataset.expressionFile, scDataset.token)
                when (val datasetExpQuery = mongoDBCollectionExp.findOne(SCDatasetExpression::token eq scExp.token)) {
                    null -> {
                        Log.info("Inserting the expression info for ${scExp.token} into the mongo database.  cur_size:${cur_file_size}MB, max_size:${max_exp_file}MB")
                        mongoDBCollectionExp.insertOne(scExp)
                    }
                    else -> {
                        Log.info("Updating the expression info for dataset ${scExp.token} in the database.  cur_size:${cur_file_size}MB, max_size:${max_exp_file}MB")
                        mongoDBCollectionExp.updateOneById(datasetExpQuery._id, scExp)
                    }

                }
            }

            if (scDataset.markersFile !== null) {
                Log.info("Updating the markers info for dataset ${scDataset.token} in the database")
                val entriesFlow = MarkerCollection.flowFromJsonFile(filePath)
                withContext(Dispatchers.IO) {
                    log.info("Updating the markers info for dataset $token in the database")
                    mongoDBCollectionMarkers.deleteMany(SCMarkerEntry::token eq scDataset.token) // Clear previous entries
                    // Collect and process each entry
                    entriesFlow.collect { (tableName, markerEntry) ->
                        val scMarkerEntry = SCMarkerEntry(
                            token = scDataset.token,
                            tableName = tableName,
                            cluster = markerEntry.cluster,
                            gene = markerEntry.gene,
                            pct1 = markerEntry.pct1,
                            pct2 = markerEntry.pct2,
                            pValue = markerEntry.pValue,
                            pValueAdjusted = markerEntry.pValueAdjusted,
                            averageLogFoldChange = markerEntry.averageLogFoldChange
                        )
                        mongoDBCollectionMarkers.insertOne(scMarkerEntry)
                    }
                }
                Log.info("Updated markers for ${scDataset.token}")
            }
        }


    } catch (e: Exception) {
        Log.error("Error while parsing $path. See exception text below")
        Log.error(e.message.toString())
    }
}






fun deleteSCDataset(path: Path,
                    mongoDBCollection: MongoCollection<SCDataset>,
                    mongoDBCollectionExp: MongoCollection<SCDatasetExpression>,
                    mongoDBCollectionMarkers: MongoCollection<SCMarkerEntry>)  {
    try {
        val datasetQuery = mongoDBCollection.findOne(SCDataset::selfPath eq path.toString())
        if (datasetQuery == null) {
            Log.info("Dataset $path was not in the database. Doing nothing")
        } else {
            Log.info("Found $path in the dataset. Removing")
            mongoDBCollection.deleteOneById(datasetQuery._id)
            mongoDBCollectionExp.deleteOne(SCDatasetExpression::token eq datasetQuery.token)
            mongoDBCollectionMarkers.deleteMany(SCMarkerEntry::token eq datasetQuery.token)
        }



    } catch (e: Exception) {
        Log.error("Something went wrong while deleting $path. See exception text below")
        Log.error(e.message.toString())
    }
}

suspend fun fileChangeHandler(modifiedChannel: Channel<Path>,
                              mongoDBCollection: MongoCollection<SCDataset>,
                              mongoDBCollectionExp: MongoCollection<SCDatasetExpression>,
                              mongoDBCollectionMarkers: MongoCollection<SCMarkerEntry>) {
    while (true) {
        val modifiedPath = modifiedChannel.receive();
        Log.info("FILE MODIFIED: $modifiedPath")


        val fileName = modifiedPath.fileName.toString()
        val datasetDir = File(modifiedPath.toString()).parentFile.parentFile.path
        val dirName = File(modifiedPath.toString()).parentFile.name

        if (fileName == DATASET_FILE_NAME) {
            insertOrUpdateSCDataset(modifiedPath, mongoDBCollection,
                mongoDBCollectionExp, mongoDBCollectionMarkers)
        } else if (dirName == FILES_FOLDER_NAME) {
            val datasetPath = Paths.get(datasetDir, DATASET_FILE_NAME)
            Log.info(datasetPath.toString())
            if (!Files.exists(datasetPath)) continue
            insertOrUpdateSCDataset(datasetPath, mongoDBCollection,
                mongoDBCollectionExp, mongoDBCollectionMarkers)
        }
    }
}

suspend fun fileDeleteHandler(deletedChannel: Channel<Path>,
                              mongoDBCollection: MongoCollection<SCDataset>,
                              mongoDBCollectionExp: MongoCollection<SCDatasetExpression>,
                              mongoDBCollectionMarkers: MongoCollection<SCMarkerEntry>) {
    while (true) {
        val deletedPath = deletedChannel.receive();
        Log.info("FILE deleted: $deletedPath")

        val fileName = deletedPath.fileName.toString()
        val dirChanged = File(deletedPath.toString()).parentFile.path
        val dirName = File(deletedPath.toString()).parentFile.name

        if (fileName == DATASET_FILE_NAME) {
            deleteSCDataset(deletedPath, mongoDBCollection,
                mongoDBCollectionExp, mongoDBCollectionMarkers)
        } else if (dirName == FILES_FOLDER_NAME) {
            val datasetPath = Paths.get(dirChanged, DATASET_FILE_NAME)
            if (!Files.exists(datasetPath)) continue
            insertOrUpdateSCDataset(datasetPath, mongoDBCollection,
                mongoDBCollectionExp, mongoDBCollectionMarkers)
        }
    }
}