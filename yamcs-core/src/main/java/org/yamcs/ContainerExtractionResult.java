package org.yamcs;

import org.yamcs.xtce.SequenceContainer;

/**
 * Holds the definition of a container, the content of its slice and some positioning information inside that slice
 */
public class ContainerExtractionResult {
    final private SequenceContainer container;
    final private byte[] containerContent;
    final private int offset;
    final private int bitPosition;

    final private long acquisitionTime;
    final private long generationTime;

    public ContainerExtractionResult(SequenceContainer container,
            byte[] containerContent,
            int offset,
            int bitPosition,
            long acquisitionTime,
            long generationTime) {
        this.container = container;
        this.containerContent = containerContent;
        this.offset = offset;
        this.bitPosition = bitPosition;
        this.acquisitionTime = acquisitionTime;
        this.generationTime = generationTime;
    }

    public SequenceContainer getContainer() {
        return container;
    }

    public byte[] getContainerContent() {
        return containerContent;
    }

    /**
     * @return the position in bits where the entries defined in this container start
     */
    public int getLocationInContainerInBits() {
        return offset * 8 + bitPosition;
    }

    /**
     * 
     * @return the position in bytes where this container including parent hierarchy starts
     */
    public int getOffset() {
        return offset;
    }

    public long getAcquisitionTime() {
        return acquisitionTime;
    }

    public long getGenerationTime() {
        return generationTime;
    }

    @Override
    public String toString() {
        return container.toString();
    }
}
