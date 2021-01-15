/*
 * Jigasi, the JItsi GAteway to SIP.
 *
 * Copyright @ 2018 - present 8x8, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jitsi.jigasi.transcription;

import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.client.*;
import org.json.*;
import org.jitsi.jigasi.*;
import org.jitsi.utils.logging.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.media.format.*;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;


/**
 * Implements a TranscriptionService which uses local
 * Vosk websocket transcription service.
 * <p>
 * See https://github.com/alphacep/vosk-server for
 * information about server
 *
 * @author Nik Vaessen
 * @author Damian Minkov
 * @author Nickolay V. Shmyrev
 */
public class AwsTranscriptionService
    implements TranscriptionService
{

    /**
     * The logger for this class
     */
    private final static Logger logger
            = Logger.getLogger(AwsTranscriptionService.class);

    /**
     * The URL of the websocket service speech-to-text service.
     */
    public final static String ACCESS_KEY_ID = "org.jitsi.jigasi.transcription.aws.access_key_id";
    public final static String SECRET_KEY_ID = "org.jitsi.jigasi.transcription.aws.secret_key_id";
    public final static String REGION = "org.jitsi.jigasi.transcription.aws.region";
    public final static String BUCKET = "org.jitsi.jigasi.transcription.aws.bucket";
    public final static String PATH = "org.jitsi.jigasi.transcription.aws.path";

    private final static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    private final AwsBasicCredentials credentials;
    private final Region region;
    private final String bucket;
    private final String path;

    /**
     * Create a TranscriptionService which will send audio to the google cloud
     * platform to get a transcription
     */
    public AwsTranscriptionService()
    {
        String accessKey = JigasiBundleActivator.getConfigurationService().getString(ACCESS_KEY_ID, "");
        String secretKey = JigasiBundleActivator.getConfigurationService().getString(SECRET_KEY_ID, "");
        String region = JigasiBundleActivator.getConfigurationService().getString(REGION, "");
        this.bucket = JigasiBundleActivator.getConfigurationService().getString(BUCKET, "");
        this.path = JigasiBundleActivator.getConfigurationService().getString(PATH, "");

        this.credentials = AwsBasicCredentials.create(accessKey, secretKey);
        this.region = Region.of(region);
    }

    /**
     * No configuration required yet
     */
    public boolean isConfiguredProperly()
    {
        return true;
    }

    /**
     * Sends audio as an array of bytes to Vosk service
     *
     * @param request        the TranscriptionRequest which holds the audio to be sent
     * @param resultConsumer a Consumer which will handle the
     *                       TranscriptionResult
     */
    @Override
    public void sendSingleRequest(final TranscriptionRequest request,
                                  final Consumer<TranscriptionResult> resultConsumer)
    {
        // Try to create the client, which can throw an IOException
        try
        {
            // Set the sampling rate and encoding of the audio
            AudioFormat format = request.getFormat();
            if (!format.getEncoding().equals("LINEAR"))
            {
                throw new IllegalArgumentException("Given AudioFormat" +
                        "has unexpected" +
                        "encoding");
            }
        }
        catch (Exception e)
        {
            logger.error("Error sending single req", e);
        }
    }

    @Override
    public StreamingRecognitionSession initStreamingSession(Participant participant)
        throws UnsupportedOperationException
    {
        try
        {
            return new AwsStreamingSession(participant);
        }
        catch (Exception e)
        {
            throw new UnsupportedOperationException("Failed to create streaming session", e);
        }
    }

    @Override
    public boolean supportsFragmentTranscription()
    {
        return true;
    }

    @Override
    public boolean supportsStreamRecognition()
    {
        return true;
    }

    /**
     * A Transcription session for transcribing streams, handles
     * the lifecycle of websocket
     */
    @WebSocket
    public class AwsStreamingSession
        implements StreamingRecognitionSession
    {
        /* The participant */
        private final Participant participant;
        private final String key;
        private final String uploadId;
        private final List<CompletedPart> parts = new ArrayList<>();

        private S3Client client;
        private int partIndex = 0;


        AwsStreamingSession(Participant participant)
            throws Exception
        {
            this.participant = participant;
            this.key = path + "/" + participant.getTranscriber().getRoomName()  + "/"
                    + dateFormatter.format(LocalDateTime.now()) + "_" + participant.getName() + ".flac";
            this.client = S3Client.builder()
                .region(region)
                .credentialsProvider(() -> credentials).build();

            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(this.key)
                    .build();

            CreateMultipartUploadResponse response = this.client.createMultipartUpload(createMultipartUploadRequest);
            this.uploadId = response.uploadId();
        }


        public void sendRequest(TranscriptionRequest request)
        {
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(++partIndex)
                    .build();
            String etag = this.client.uploadPart(uploadPartRequest, RequestBody.fromBytes(request.getAudio())).eTag();

            CompletedPart endPart = CompletedPart.builder()
                    .partNumber(partIndex)
                    .eTag(etag)
                    .build();

            parts.add(endPart);
        }

        public void end()
        {
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                    .parts(parts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(this.key)
                    .uploadId(this.uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();

            this.client.completeMultipartUpload(completeMultipartUploadRequest);

            this.client.close();
            this.client = null;
        }

        public boolean ended()
        {
            return client == null;
        }

        @Override
        public void addTranscriptionListener(TranscriptionListener listener) {
            //TODO: Lorsque on affichera du texte
        }
    }



}
