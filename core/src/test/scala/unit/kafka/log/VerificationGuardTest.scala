package unit.kafka.log

import org.apache.kafka.storage.internals.log.VerificationGuard
import org.apache.kafka.storage.internals.log.VerificationGuard.SENTINEL_VERIFICATION_GUARD
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}
import org.junit.jupiter.api.Test

class VerificationGuardTest {

  @Test
  def testEqualsAndHashCode(): Unit = {
    val verificationGuard1 = new VerificationGuard
    val verificationGuard2 = new VerificationGuard

    assertNotEquals(verificationGuard1, verificationGuard2)
    assertNotEquals(SENTINEL_VERIFICATION_GUARD, verificationGuard1)
    assertEquals(SENTINEL_VERIFICATION_GUARD, SENTINEL_VERIFICATION_GUARD)

    assertNotEquals(verificationGuard1.hashCode, verificationGuard2.hashCode)
    assertNotEquals(SENTINEL_VERIFICATION_GUARD.hashCode, verificationGuard1.hashCode)
    assertEquals(SENTINEL_VERIFICATION_GUARD.hashCode, SENTINEL_VERIFICATION_GUARD.hashCode)
  }

  @Test
  def testVerifiedBy(): Unit = {
    val verificationGuard1 = new VerificationGuard
    val verificationGuard2 = new VerificationGuard

    assertFalse(verificationGuard1.verifiedBy(verificationGuard2))
    assertFalse(verificationGuard1.verifiedBy(SENTINEL_VERIFICATION_GUARD))
    assertFalse(SENTINEL_VERIFICATION_GUARD.verifiedBy(verificationGuard1))
    assertFalse(SENTINEL_VERIFICATION_GUARD.verifiedBy(SENTINEL_VERIFICATION_GUARD))
    assertTrue(verificationGuard1.verifiedBy(verificationGuard1))
  }

}
