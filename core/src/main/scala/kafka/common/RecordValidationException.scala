package kafka.common

import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.requests.ProduceResponse.ErrorRecord

class RecordValidationException(val invalidException: ApiException,
                                val errorRecords: List[ErrorRecord]) extends RuntimeException {
}
