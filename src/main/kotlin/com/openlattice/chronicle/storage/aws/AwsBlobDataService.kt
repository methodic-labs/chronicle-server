package com.openlattice.chronicle.storage.aws

import com.google.common.util.concurrent.ListeningExecutorService
import com.openlattice.chronicle.configuration.ChronicleConfiguration
import com.openlattice.chronicle.storage.BinaryObjectWithMetadata
import com.openlattice.chronicle.storage.ByteBlobDataManager
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest
import java.net.URL
import java.time.Duration
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Semaphore


private val logger = LoggerFactory.getLogger(AwsBlobDataService::class.java)
const val MAX_ERROR_RETRIES = 5
private const val MAX_NUM_OF_OBJECTS_FOR_S3_DELETE = 1000
private val MAX_PARALLEL_JOBS = Runtime.getRuntime().availableProcessors()

@Service
class AwsBlobDataService(
        private val datastoreConfiguration: ChronicleConfiguration,
        private val executorService: ListeningExecutorService
) : ByteBlobDataManager {

    private val credentials = AwsBasicCredentials.create(
        datastoreConfiguration.accessKeyId,
        datastoreConfiguration.secretAccessKey
    )
    private val credentialsProvider = StaticCredentialsProvider.create(credentials)
    private val s3 = newS3Client(datastoreConfiguration)
    private val presigner = S3Presigner.builder()
        .region(Region.of(datastoreConfiguration.regionName))
        .credentialsProvider(credentialsProvider)
        .build()
    private val semaphore = Semaphore(MAX_PARALLEL_JOBS)

    private fun newS3Client(datastoreConfiguration: ChronicleConfiguration): S3Client {
        return S3Client.builder()
            .region(Region.of(datastoreConfiguration.regionName))
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration {
                it.retryPolicy(RetryPolicy.builder().numRetries(MAX_ERROR_RETRIES).build())
            }
            .build()
    }

    override fun putObject(s3Key: String, binaryObjectWithMetadata: BinaryObjectWithMetadata) {
        val builder = PutObjectRequest.builder()
            .bucket(datastoreConfiguration.bucketName)
            .key(s3Key)
            .contentType(binaryObjectWithMetadata.contentType)

        binaryObjectWithMetadata.contentDisposition?.let { builder.contentDisposition(it) }

        val data = binaryObjectWithMetadata.data
        s3.putObject(builder.build(), RequestBody.fromBytes(data))
    }

    override fun deleteObjects(s3Keys: List<String>) {
        s3Keys.chunked(MAX_NUM_OF_OBJECTS_FOR_S3_DELETE).map { s3KeyBatch ->
            val keysToDelete = s3KeyBatch.map { ObjectIdentifier.builder().key(it).build() }
            val deleteRequest = DeleteObjectsRequest.builder()
                .bucket(datastoreConfiguration.bucketName)
                .delete(Delete.builder().objects(keysToDelete).build())
                .build()

            try {
                semaphore.acquire()
                executorService.execute {
                    try {
                        s3.deleteObjects(deleteRequest)
                    } finally {
                        semaphore.release()
                    }
                }
            } catch (ex: Exception) {
                logger.error("Error while attempting to delete from S3.", ex)
                semaphore.release()
            }
        }
    }

    override fun deleteObject(s3Key: String) {
        val deleteRequest = DeleteObjectRequest.builder()
            .bucket(datastoreConfiguration.bucketName)
            .key(s3Key)
            .build()
        s3.deleteObject(deleteRequest)
    }

    override fun getObjects(keys: Collection<Any>): List<Any> {
        return getPresignedUrls(keys)
    }

    override fun getPresignedUrls(keys: Collection<Any>): List<URL> {
        return getPresignedUrlsWithDispositions(keys.associate { it as String to null }).values.toList()
    }

    override fun getPresignedUrlsWithDispositions(keysToDispositions: Map<String, String?>): Map<String, URL> {
        val ttlMillis = datastoreConfiguration.timeToLive

        return keysToDispositions
            .map { (key, disposition) ->
                executorService.submit(Callable<Pair<String, URL>> {
                    key to getPresignedUrl(
                        key = key,
                        expiration = Date(System.currentTimeMillis() + ttlMillis),
                        httpMethod = "GET",
                        contentDisposition = disposition
                    )
                })
            }.map { it.get() }.toMap()
    }

    override fun getPresignedUrl(
        key: Any,
        expiration: Date,
        httpMethod: String,
        contentType: String?,
        contentDisposition: String?
    ): URL {
        val durationMillis = expiration.time - System.currentTimeMillis()
        val duration = Duration.ofMillis(maxOf(durationMillis, 1000))

        return try {
            val getObjectRequest = GetObjectRequest.builder()
                .bucket(datastoreConfiguration.bucketName)
                .key(key.toString())

            contentDisposition?.let { getObjectRequest.responseContentDisposition(it) }
            contentType?.let { getObjectRequest.responseContentType(it) }

            val presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(duration)
                .getObjectRequest(getObjectRequest.build())
                .build()

            presigner.presignGetObject(presignRequest).url()
        } catch (e: S3Exception) {
            logger.warn("Amazon couldn't process call", e)
            throw e
        }
    }

    override fun getDefaultExpirationDateTime(): Date {
        val expirationTime = Date()
        val timeToLive = expirationTime.time + datastoreConfiguration.timeToLive
        expirationTime.time = timeToLive
        return expirationTime
    }
}
