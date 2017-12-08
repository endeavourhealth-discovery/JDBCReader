package org.endeavourhealth.jdbcreader;

import org.apache.commons.io.FilenameUtils;

public class LocalDataFile {

    private String localRootPathPrefix;
    private String localRootPath;
    private String batchIdentifier;
    private String fileName;
    private String fileTypeIdentifier;
    private Integer batchFileId = null;

    public LocalDataFile() {
    }

    public String getLocalPath() {
        return FilenameUtils.concat(this.localRootPathPrefix, this.localRootPath);
    }

    public String getLocalPathBatch() {
        return FilenameUtils.concat(getLocalPath(), this.getBatchIdentifier());
    }

    public String getLocalPathFile() {
        return FilenameUtils.concat(getLocalPathBatch(), getFileName());
    }

    public String getLocalRootPathPrefix() {
        return localRootPathPrefix;
    }

    public void setLocalRootPathPrefix(String localRootPathPrefix) {
        this.localRootPathPrefix = localRootPathPrefix;
    }

    public String getLocalRootPath() {
        return localRootPath;
    }

    public void setLocalRootPath(String localRootPath) {
        this.localRootPath = localRootPath;
    }

    public String getBatchIdentifier() {
        return batchIdentifier;
    }

    public void setBatchIdentifier(String batchIdentifier) {
        this.batchIdentifier = batchIdentifier;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileTypeIdentifier() {
        return fileTypeIdentifier;
    }

    public void setFileTypeIdentifier(String fileTypeIdentifier) {
        this.fileTypeIdentifier = fileTypeIdentifier;
    }

    public Integer getBatchFileId() {
        return batchFileId;
    }

    public void setBatchFileId(Integer batchFileId) {
        this.batchFileId = batchFileId;
    }
}
