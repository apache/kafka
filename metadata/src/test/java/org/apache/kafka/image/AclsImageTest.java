/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.metadata.authorizer.StandardAclWithIdTest.TEST_ACLS;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class AclsImageTest {
    public final static AclsImage IMAGE1;

    public final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static AclsDelta DELTA1;

    final static AclsImage IMAGE2;

    static {
        Map<Uuid, StandardAcl> map = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            StandardAclWithId aclWithId = TEST_ACLS.get(i);
            map.put(aclWithId.id(), aclWithId.acl());
        }
        IMAGE1 = new AclsImage(map);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveAccessControlEntryRecord().
            setId(Uuid.fromString("QZDDv-R7SyaPgetDPGd0Mw")), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(TEST_ACLS.get(4).toRecord(), (short) 0));

        DELTA1 = new AclsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);


        Map<Uuid, StandardAcl> map2 = new HashMap<>();
        for (int i = 1; i < 5; i++) {
            StandardAclWithId aclWithId = TEST_ACLS.get(i);
            map2.put(aclWithId.id(), aclWithId.acl());
        }
        IMAGE2 = new AclsImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() {
        testToImage(AclsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() {
        testToImage(IMAGE1);
    }

    @Test
    public void testApplyDelta1() {
        assertEquals(IMAGE2, DELTA1.apply());
        // check image1 + delta1 = image2, since records for image1 + delta1 might differ from records from image2
        List<ApiMessageAndVersion> records = getImageRecords(IMAGE1);
        records.addAll(DELTA1_RECORDS);
        testToImage(IMAGE2, records);
    }

    @Test
    public void testImage2RoundTrip() {
        testToImage(IMAGE2);
    }

    private static void testToImage(AclsImage image) {
        testToImage(image, Optional.empty());
    }

    private static void testToImage(AclsImage image, Optional<List<ApiMessageAndVersion>> fromRecords) {
        testToImage(image, fromRecords.orElseGet(() -> getImageRecords(image)));
    }

    private static void testToImage(AclsImage image, List<ApiMessageAndVersion> fromRecords) {
        // test from empty image stopping each of the various intermediate images along the way
        new RecordTestUtils.TestThroughAllIntermediateImagesLeadingToFinalImageHelper() {

            @Override
            public Object getEmptyImage() {
                return AclsImage.EMPTY;
            }

            @Override
            public Object createDeltaUponImage(Object image) {
                return new AclsDelta((AclsImage) image);
            }

            @Override
            public Object createImageByApplyingDelta(Object delta) {
                return ((AclsDelta) delta).apply();
            }
        }.test(image, fromRecords);
    }

    private static List<ApiMessageAndVersion> getImageRecords(AclsImage image) {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        return writer.records();
    }

    private static void testThroughAllIntermediateImagesLeadingToFinalImage(AclsImage finalImage, List<ApiMessageAndVersion> fromRecords) {
        for (int numRecordsForfirstImage = 1; numRecordsForfirstImage <= fromRecords.size(); ++numRecordsForfirstImage) {
            // create first image from first numRecordsForfirstImage records
            AclsDelta delta = new AclsDelta(AclsImage.EMPTY);
            RecordTestUtils.replayAll(delta, fromRecords.subList(0, numRecordsForfirstImage));
            AclsImage firstImage = delta.apply();
            // for all possible further batch sizes, apply as many batches as it takes to get to the final image
            int remainingRecords = fromRecords.size() - numRecordsForfirstImage;
            if (remainingRecords == 0) {
                assertEquals(finalImage, firstImage);
            } else {
                // for all possible further batch sizes...
                for (int maxRecordsForSuccessiveBatches = 1; maxRecordsForSuccessiveBatches <= remainingRecords; ++maxRecordsForSuccessiveBatches) {
                    AclsImage latestIntermediateImage = firstImage;
                    // ... apply as many batches as it takes to get to the final image
                    int numAdditionalBatches = (int) Math.ceil(remainingRecords * 1.0 / maxRecordsForSuccessiveBatches);
                    for (int additionalBatchNum = 0; additionalBatchNum < numAdditionalBatches; ++additionalBatchNum) {
                        // apply up to maxRecordsForSuccessiveBatches records on top of the latest intermediate image
                        // to obtain the next intermediate image.
                        delta = new AclsDelta(latestIntermediateImage);
                        int applyFromIndex = numRecordsForfirstImage + additionalBatchNum * maxRecordsForSuccessiveBatches;
                        int applyToIndex = Math.min(fromRecords.size(), applyFromIndex + maxRecordsForSuccessiveBatches);
                        RecordTestUtils.replayAll(delta, fromRecords.subList(applyFromIndex, applyToIndex));
                        latestIntermediateImage = delta.apply();
                    }
                    // The final intermediate image received should be the expected final image
                    assertEquals(finalImage, latestIntermediateImage);
                }
            }
        }
    }
}
