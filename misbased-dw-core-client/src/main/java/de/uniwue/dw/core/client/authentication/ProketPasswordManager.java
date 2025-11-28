package de.uniwue.dw.core.client.authentication;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public class ProketPasswordManager {
  private static final SecureRandom random = new SecureRandom();

  private static final int ITERATIONS = 10000;

  private static final int KEY_LENGTH = 128;

  public static String generateSalt() {
    final byte[] salt = new byte[16];
    random.nextBytes(salt);
    return Base64.getEncoder().encodeToString(salt);
  }

  public static String hash(String password, String salt) {
    char[] passwordChars = password.toCharArray();
    final PBEKeySpec spec = new PBEKeySpec(passwordChars, Base64.getDecoder().decode(salt),
            ITERATIONS, KEY_LENGTH);
    wipe(passwordChars);
    try {
      final SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
      byte[] encodingResult = factory.generateSecret(spec).getEncoded();
      return Base64.getEncoder().encodeToString(encodingResult);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new RuntimeException("Failure while trying to hash password.", e);
    } finally {
      spec.clearPassword();
    }
  }

  private static void wipe(char[] array) {
    Arrays.fill(array, Character.MAX_VALUE);
  }
}
