package com.lakala.datacenter.core.models;

public class PartitionDescription {
    private Long partitionId;
    private String partitionLabel;
    private String groupRelationship;
    private String targetRelationship;

    public String getPartitionLabel() {
        return partitionLabel;
    }

    public void setPartitionLabel(String partitionLabel) {
        this.partitionLabel = partitionLabel;
    }

    public Long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Long partitionId) {
        this.partitionId = partitionId;
    }

    public String getTargetRelationship() {
        return targetRelationship;
    }

    public void setTargetRelationship(String targetRelationship) {
        this.targetRelationship = targetRelationship;
    }

    public String getGroupRelationship() {
        return groupRelationship;
    }

    public void setGroupRelationship(String groupRelationship) {
        this.groupRelationship = groupRelationship;
    }

    public PartitionDescription(Long partitionId, String partitionLabel) {
        this.partitionId = partitionId;
        this.partitionLabel = partitionLabel;
    }
}
