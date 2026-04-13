package com.simplekafka.broker;

import java.io.*;
import java.nio.file.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Simulates a Cloud Object Storage (S3) bucket for Tiered Storage capabilities.
 * In a real environment, this would use the AWS Java SDK to connect to Amazon S3. 
 * Here, it simulates cloud transmission by compressing log/index segments 
 * to heavily reduced ZIP archives and moving them to an isolated boundary.
 */
public class MockS3TieredStorage {
    private static final Logger LOGGER = Logger.getLogger(MockS3TieredStorage.class.getName());
    public static final String CLOUD_BUCKET_DIR = ".cloud_bucket";

    public MockS3TieredStorage() {
        File bucket = new File(CLOUD_BUCKET_DIR);
        if (!bucket.exists()) {
            bucket.mkdirs();
        }
    }

    /**
     * Upload physical segment files to the Cloud Bucket via Zip Compression.
     * @param logPath local .log file
     * @param indexPath local .index file
     * @param cloudKey unique identifier for S3 retrieval
     * @return true if upload was successful
     */
    public boolean uploadSegment(String logPath, String indexPath, String cloudKey) {
        String destZipPath = CLOUD_BUCKET_DIR + File.separator + cloudKey + ".zip";
        
        try (FileOutputStream fos = new FileOutputStream(destZipPath);
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            
            // Compress Log
            compressFileToZip(logPath, zos);
            // Compress Index
            compressFileToZip(indexPath, zos);

            LOGGER.info("[CLOUD-UP] Successfully archived segment to S3 Tier: " + destZipPath);
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[CLOUD-UP-ERROR] Failed to archive to S3 Tier", e);
            return false;
        }
    }

    private void compressFileToZip(String filePath, ZipOutputStream zos) throws IOException {
        File fileToZip = new File(filePath);
        if (!fileToZip.exists()) return;

        try (FileInputStream fis = new FileInputStream(fileToZip)) {
            // Give original file name to zip entry
            ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
            zos.putNextEntry(zipEntry);

            byte[] bytes = new byte[4096];
            int length;
            while ((length = fis.read(bytes)) >= 0) {
                zos.write(bytes, 0, length);
            }
            zos.closeEntry();
        }
    }

    /**
     * Download segment compressed zip file from Cloud and unarchive it into standard .log files.
     * @param cloudKey identifier of S3 zip bucket
     * @param extractionDir directory to place temporary unarchived logs
     * @return true if successful download and extraction
     */
    public boolean downloadSegment(String cloudKey, String extractionDir) {
        String targetZip = CLOUD_BUCKET_DIR + File.separator + cloudKey + ".zip";
        File arch = new File(targetZip);
        
        if (!arch.exists()) {
            LOGGER.warning("[CLOUD-DOWN-ERROR] Cloud key does not exist: " + targetZip);
            return false;
        }

        File outDir = new File(extractionDir);
        if(!outDir.exists()) outDir.mkdirs();

        try (FileInputStream fis = new FileInputStream(targetZip);
             ZipInputStream zis = new ZipInputStream(fis)) {
            
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                File newFile = new File(extractionDir, zipEntry.getName());
                
                try (FileOutputStream fos = new FileOutputStream(newFile)) {
                    byte[] buffer = new byte[4096];
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                }
                
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
            LOGGER.info("[CLOUD-DOWN] Successfully pulled and unarchived segment from S3: " + cloudKey);
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[CLOUD-DOWN-ERROR] Failed to download from S3 Tier", e);
            return false;
        }
    }
}
