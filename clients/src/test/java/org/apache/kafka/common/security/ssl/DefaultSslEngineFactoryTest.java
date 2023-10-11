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
package org.apache.kafka.common.security.ssl;

import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@SuppressWarnings("this-escape")
public class DefaultSslEngineFactoryTest {

    /*
     * Key and certificates were extracted using openssl from a key store file created with 100 years validity using:
     *
     * openssl pkcs12 -in server.keystore.p12 -nodes -nocerts -out test.key.pem -passin pass:key-password
     * openssl pkcs12 -in server.keystore.p12 -nodes -nokeys -out test.certchain.pem  -passin pass:key-password
     * openssl pkcs12 -in server.keystore.p12 -nodes  -out test.keystore.pem -passin pass:key-password
     * openssl pkcs8 -topk8 -v1 pbeWithSHA1And3-KeyTripleDES-CBC -in test.key.pem -out test.key.encrypted.pem -passout pass:key-password
     */

    private static final String CA1 = "-----BEGIN CERTIFICATE-----\n"
            + "MIIC0zCCAbugAwIBAgIEStdXHTANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU\n"
            + "ZXN0Q0ExMCAXDTIwMDkyODA5MDI0MFoYDzIxMjAwOTA0MDkwMjQwWjASMRAwDgYD\n"
            + "VQQDEwdUZXN0Q0ExMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo3Gr\n"
            + "WJAkjnvgcuIfjArDhNdtAlRTt094WMUXhYDibgGtd+CLcWqA+c4PEoK4oybnKZqU\n"
            + "6MlDfPgesIK2YiNBuSVWMtZ2doageOBnd80Iwbg8DqGtQpUsvw8X5fOmuza+4inv\n"
            + "/8IpiTizq8YjSMT4nYDmIjyyRCSNY4atjgMnskutJ0v6i69+ZAA520Y6nn2n4RD5\n"
            + "8Yc+y7yCkbZXnYS5xBOFEExmtc0Xa7S9nM157xqKws9Z+rTKZYLrryaHI9JNcXgG\n"
            + "kzQEH9fBePASeWfi9AGRvAyS2GMSIBOsihIDIha/mqQcJOGCEqTMtefIj2FaErO2\n"
            + "bL9yU7OpW53iIC8y0QIDAQABoy8wLTAMBgNVHRMEBTADAQH/MB0GA1UdDgQWBBRf\n"
            + "svKcoQ9ZBvjwyUSV2uMFzlkOWDANBgkqhkiG9w0BAQsFAAOCAQEAEE1ZG2MGE248\n"
            + "glO83ROrHbxmnVWSQHt/JZANR1i362sY1ekL83wlhkriuvGVBlHQYWezIfo/4l9y\n"
            + "JTHNX3Mrs9eWUkaDXADkHWj3AyLXN3nfeU307x1wA7OvI4YKpwvfb4aYS8RTPz9d\n"
            + "JtrfR0r8aGTgsXvCe4SgwDBKv7bckctOwD3S7D/b6y3w7X0s7JCU5+8ZjgoYfcLE\n"
            + "gNqQEaOwdT2LHCvxHmGn/2VGs/yatPQIYYuufe5i8yX7pp4Xbd2eD6LULYkHFs3x\n"
            + "uJzMRI7BukmIIWuBbAkYI0atxLQIysnVFXdL9pBgvgso2nA3FgP/XeORhkyHVvtL\n"
            + "REH2YTlftQ==\n"
            + "-----END CERTIFICATE-----";

    private static final String CA2 = "-----BEGIN CERTIFICATE-----\n"
            + "MIIC0zCCAbugAwIBAgIEfk9e9DANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU\n"
            + "ZXN0Q0EyMCAXDTIwMDkyODA5MDI0MVoYDzIxMjAwOTA0MDkwMjQxWjASMRAwDgYD\n"
            + "VQQDEwdUZXN0Q0EyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvCh0\n"
            + "UO5op9eHfz7mvZ7IySK7AOCTC56QYFJcU+hD6yk1wKg2qot7naI5ozAc8n7c4pMt\n"
            + "LjI3D0VtC/oHC29R2HNMSWyHcxIXw8z127XeCLRkCqYWuVAl3nBuWfWVPObjKetH\n"
            + "TWlQANYWAfk1VbS6wfzgp9cMaK7wQ+VoGEo4x3pjlrdlyg4k4O2yubcpWmJ2TjxS\n"
            + "gg7TfKGizUVAvF9wUG9Q4AlCg4uuww5RN9w6vnzDKGhWJhkQ6pf/m1xB+WueFOeU\n"
            + "aASGhGqCTqiz3p3M3M4OZzG3KptjQ/yb67x4T5U5RxqoiN4L57E7ZJLREpa6ZZNs\n"
            + "ps/gQ8dR9Uo/PRyAkQIDAQABoy8wLTAMBgNVHRMEBTADAQH/MB0GA1UdDgQWBBRg\n"
            + "IAOVH5LeE6nZmdScEE3JO/AhvTANBgkqhkiG9w0BAQsFAAOCAQEAHkk1iybwy/Lf\n"
            + "iEQMVRy7XfuC008O7jfCUBMgUvE+oO2RadH5MmsXHG3YerdsDM90dui4JqQNZOUh\n"
            + "kF8dIWPQHE0xDsR9jiUsemZFpVMN7DcvVZ3eFhbvJA8Q50rxcNGA+tn9xT/xdQ6z\n"
            + "1eRq9IPoYcRexQ7s9mincM4T4lLm8GGcd7ZPHy8kw0Bp3E/enRHWaF5b8KbXezXD\n"
            + "I3SEYUyRL2K3px4FImT4X9XQm2EX6EONlu4GRcJpD6RPc0zC7c9dwEnSo+0NnewR\n"
            + "gjgO34CLzShB/kASLS9VQXcUC6bsggAVK2rWQMmy35SOEUufSuvg8kUFoyuTzfhn\n"
            + "hL+PVwIu7g==\n"
            + "-----END CERTIFICATE-----";

    private static final String CERTCHAIN = "Bag Attributes\n"
            + "    friendlyName: server\n"
            + "    localKeyID: 54 69 6D 65 20 31 36 30 31 32 38 33 37 36 35 34 32 33 \n"
            + "subject=/CN=TestBroker\n"
            + "issuer=/CN=TestCA1\n"
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIIC/zCCAeegAwIBAgIEatBnEzANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU\n"
            + "ZXN0Q0ExMCAXDTIwMDkyODA5MDI0NFoYDzIxMjAwOTA0MDkwMjQ0WjAVMRMwEQYD\n"
            + "VQQDEwpUZXN0QnJva2VyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n"
            + "pkw1AS71ej/iOMvzVgVL1dkQOYzI842NcPmx0yFFsue2umL8WVd3085NgWRb3SS1\n"
            + "4X676t7zxjPGzYi7jwmA8stCrDt0NAPWd/Ko6ErsCs87CUs4u1Cinf+b3o9NF5u0\n"
            + "UPYBQLF4Ir8T1jQ+tKiqsChGDt6urRAg1Cro5i7r10jN1uofY2tBs+r8mALhJ17c\n"
            + "T5LKawXeYwNOQ86c5djClbcP0RrfcPyRyj1/Cp1axo28iO0fXFyO2Zf3a4vtt+Ih\n"
            + "PW+A2tL+t3JTBd8g7Fl3ozzpcotAi7MDcZaYA9GiTP4DOiKUeDt6yMYQQr3VEqGa\n"
            + "pXp4fKY+t9slqnAmcBZ4kQIDAQABo1gwVjAfBgNVHSMEGDAWgBRfsvKcoQ9ZBvjw\n"
            + "yUSV2uMFzlkOWDAUBgNVHREEDTALgglsb2NhbGhvc3QwHQYDVR0OBBYEFGWt+27P\n"
            + "INk/S5X+PRV/jW3WOhtaMA0GCSqGSIb3DQEBCwUAA4IBAQCLHCjFFvqa+0GcG9eq\n"
            + "v1QWaXDohY5t5CCwD8Z+lT9wcSruTxDPwL7LrR36h++D6xJYfiw4iaRighoA40xP\n"
            + "W6+0zGK/UtWV4t+ODTDzyAWgls5w+0R5ki6447qGqu5tXlW5DCHkkxWiozMnhNU2\n"
            + "G3P/Drh7DhmADDBjtVLsu5M1sagF/xwTP/qCLMdChlJNdeqyLnAUa9SYG1eNZS/i\n"
            + "wrCC8m9RUQb4+OlQuFtr0KhaaCkBXfmhigQAmh44zSyO+oa3qQDEavVFo/Mcui9o\n"
            + "WBYetcgVbXPNoti+hQEMqmJYBHlLbhxMnkooGn2fa70f453Bdu/Xh6Yphi5NeCHn\n"
            + "1I+y\n"
            + "-----END CERTIFICATE-----\n"
            + "Bag Attributes\n"
            + "    friendlyName: CN=TestCA1\n"
            + "subject=/CN=TestCA1\n"
            + "issuer=/CN=TestCA1\n"
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIIC0zCCAbugAwIBAgIEStdXHTANBgkqhkiG9w0BAQsFADASMRAwDgYDVQQDEwdU\n"
            + "ZXN0Q0ExMCAXDTIwMDkyODA5MDI0MFoYDzIxMjAwOTA0MDkwMjQwWjASMRAwDgYD\n"
            + "VQQDEwdUZXN0Q0ExMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo3Gr\n"
            + "WJAkjnvgcuIfjArDhNdtAlRTt094WMUXhYDibgGtd+CLcWqA+c4PEoK4oybnKZqU\n"
            + "6MlDfPgesIK2YiNBuSVWMtZ2doageOBnd80Iwbg8DqGtQpUsvw8X5fOmuza+4inv\n"
            + "/8IpiTizq8YjSMT4nYDmIjyyRCSNY4atjgMnskutJ0v6i69+ZAA520Y6nn2n4RD5\n"
            + "8Yc+y7yCkbZXnYS5xBOFEExmtc0Xa7S9nM157xqKws9Z+rTKZYLrryaHI9JNcXgG\n"
            + "kzQEH9fBePASeWfi9AGRvAyS2GMSIBOsihIDIha/mqQcJOGCEqTMtefIj2FaErO2\n"
            + "bL9yU7OpW53iIC8y0QIDAQABoy8wLTAMBgNVHRMEBTADAQH/MB0GA1UdDgQWBBRf\n"
            + "svKcoQ9ZBvjwyUSV2uMFzlkOWDANBgkqhkiG9w0BAQsFAAOCAQEAEE1ZG2MGE248\n"
            + "glO83ROrHbxmnVWSQHt/JZANR1i362sY1ekL83wlhkriuvGVBlHQYWezIfo/4l9y\n"
            + "JTHNX3Mrs9eWUkaDXADkHWj3AyLXN3nfeU307x1wA7OvI4YKpwvfb4aYS8RTPz9d\n"
            + "JtrfR0r8aGTgsXvCe4SgwDBKv7bckctOwD3S7D/b6y3w7X0s7JCU5+8ZjgoYfcLE\n"
            + "gNqQEaOwdT2LHCvxHmGn/2VGs/yatPQIYYuufe5i8yX7pp4Xbd2eD6LULYkHFs3x\n"
            + "uJzMRI7BukmIIWuBbAkYI0atxLQIysnVFXdL9pBgvgso2nA3FgP/XeORhkyHVvtL\n"
            + "REH2YTlftQ==\n"
            + "-----END CERTIFICATE-----";

    private static final String KEY =  "Bag Attributes\n"
            + "    friendlyName: server\n"
            + "    localKeyID: 54 69 6D 65 20 31 36 30 31 32 38 33 37 36 35 34 32 33\n"
            + "Key Attributes: <No Attributes>\n"
            + "-----BEGIN PRIVATE KEY-----\n"
            + "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCmTDUBLvV6P+I4\n"
            + "y/NWBUvV2RA5jMjzjY1w+bHTIUWy57a6YvxZV3fTzk2BZFvdJLXhfrvq3vPGM8bN\n"
            + "iLuPCYDyy0KsO3Q0A9Z38qjoSuwKzzsJSzi7UKKd/5vej00Xm7RQ9gFAsXgivxPW\n"
            + "ND60qKqwKEYO3q6tECDUKujmLuvXSM3W6h9ja0Gz6vyYAuEnXtxPksprBd5jA05D\n"
            + "zpzl2MKVtw/RGt9w/JHKPX8KnVrGjbyI7R9cXI7Zl/dri+234iE9b4Da0v63clMF\n"
            + "3yDsWXejPOlyi0CLswNxlpgD0aJM/gM6IpR4O3rIxhBCvdUSoZqlenh8pj632yWq\n"
            + "cCZwFniRAgMBAAECggEAOfC/XwQvf0KW3VciF0yNGZshbgvBUCp3p284J+ml0Smu\n"
            + "ns4yQiaZl3B/zJ9c6nYJ8OEpNDIuGVac46vKPZIAHZf4SO4GFMFpji078IN6LmH5\n"
            + "nclZoNn9brNKaYbgQ2N6teKgmRu8Uc7laHKXjnZd0jaWAkRP8/h0l7fDob+jaERj\n"
            + "oJBx4ux2Z62TTCP6W4VY3KZgSL1p6dQswqlukPVytMeI2XEwWnO+w8ED0BxCxM4F\n"
            + "K//dw7nUMGS9GUNkgyDcH1akYSCDzdBeymQBp2latBotVfGNK1hq9nC1iaxmRkJL\n"
            + "sYjwVc24n37u+txOovy3daq2ySj9trF7ySAPVYkh4QKBgQDWeN/MR6cy1TLF2j3g\n"
            + "eMMeM32LxXArIPsar+mft+uisKWk5LDpsKpph93sl0JjFi4x0t1mqw23h23I+B2c\n"
            + "JWiPAHUG3FGvvkPPcfMUvd7pODyE2XaXi+36UZAH7qc94VZGJEb+sPITckSruREE\n"
            + "QErWZyrbBRgvQXsmVme5B2/kRQKBgQDGf2HQH0KHl54O2r9vrhiQxWIIMSWlizJC\n"
            + "hjboY6DkIsAMwnXp3wn3Bk4tSgeLk8DEVlmEaE3gvGpiIp0vQnSOlME2TXfEthdM\n"
            + "uS3+BFXN4Vxxx/qjKL2WfZloyzdaaaF7s+LIwmXgLsFFCUSq+uLtBqfpH2Qv+paX\n"
            + "Xqm7LN3V3QKBgH5ssj/Q3RZx5oQKqf7wMNRUteT2dbB2uI56s9SariQwzPPuevrG\n"
            + "US30ETWt1ExkfsaP7kLfAi71fhnBaHLq+j+RnWp15REbrw1RtmC7q/L+W25UYjvj\n"
            + "GF0+RxDl9V/cvOaL6+2mkIw2B5TSet1uqK7KEdEZp6/zgYyP0oSXhbWhAoGAdnlZ\n"
            + "HCtMPjnUcPFHCZVTvDTTSihrW9805FfPNe0g/olvLy5xymEBRZtR1d41mq1ZhNY1\n"
            + "H75RnS1YIbKfNrHnd6J5n7ulHJfCWFy+grp7rCIyVwcRJYkPf17/zXhdVW1uoLLB\n"
            + "TSoaPDAr0tSxU4vjHa23UoEV/z0F3Nr3W2xwC1ECgYBHKjv6ekLhx7HbP797+Ai+\n"
            + "wkHvS2L/MqEBxuHzcQ9G6Mj3ANAeyDB8YSC8qGtDQoEyukv2dO73lpodNgbR8P+Q\n"
            + "PDBb6eyntAo2sSeo0jZkiXvDOfRaGuGVrxjuTfaqcVB33jC6BYfi61/3Sr5oG9Nd\n"
            + "tDGh1HlOIRm1jD9KQNVZ/Q==\n"
            + "-----END PRIVATE KEY-----";

    private static final String ENCRYPTED_KEY =  "-----BEGIN ENCRYPTED PRIVATE KEY-----\n"
            + "MIIE6jAcBgoqhkiG9w0BDAEDMA4ECGyAEWAXlaXzAgIIAASCBMgt7QD1Bbz7MAHI\n"
            + "Ni0eTrwNiuAPluHirLXzsV57d1O9i4EXVp5nzRy6753cjXbGXARbBeaJD+/+jbZp\n"
            + "CBZTHMG8rTCfbsg5kMqxT6XuuqWlKLKc4gaq+QNgHHleKqnpwZQmOQ+awKWEK/Ow\n"
            + "Z0KxXqkp+b4/qJK3MqKZDsJtVdyUhO0tLVxd+BHDg9B93oExc87F16h3R0+T4rxE\n"
            + "Tvz2c2upBqva49AbLDxpWXLCJC8CRkxM+KHrPkYjpNx3jCjtwiiXfzJCWjuCkVrL\n"
            + "2F4bqvpYPIseoPtMvWaplNtoPwhpzBB/hoJ+R+URr4XHX3Y+bz6k6iQnhoCOIviy\n"
            + "oEEUvWtKnaEEKSauR+Wyj3MoeB64g9NWMEHv7+SQeA4WqlgV2s4txwRxFGKyKLPq\n"
            + "caMSpfxvYujtSh0DOv9GI3cVHPM8WsebCz9cNrbKSR8/8JufcoonTitwF/4vm1Et\n"
            + "AdmCuH9JIYVvmFKFVxY9SvRAvo43OQaPmJQHMUa4yDfMtpTSgmB/7HFgxtksYs++\n"
            + "Gbrq6F/hon+0bLx+bMz2FK635UU+iVno+qaScKWN3BFqDl+KnZprBhLSXTT3aHmp\n"
            + "fisQit/HWp71a0Vzq85WwI4ucMKNc8LemlwNBxWLLiJDp7sNPLb5dIl8yIwSEIgd\n"
            + "vC5px9KWEdt3GxTUEqtIeBmagbBhahcv+c9Dq924DLI+Slv6TJKZpIcMqUECgzvi\n"
            + "hb8gegyEscBEcDSzl0ojlFVz4Va5eZS/linTjNJhnkx8BKLn/QFco7FpEE6uOmQ3\n"
            + "0kF64M2Rv67cJbYVrhD46TgIzH3Y/FOMSi1zFHQ14nVXWMu0yAlBX+QGk7Xl+/aF\n"
            + "BIq+i9WcBqbttR3CwyeTnIFXkdC66iTZYhDl9HT6yMcazql2Or2TjIIWr6tfNWH/\n"
            + "5dWSEHYM5m8F2/wF0ANWJyR1oPr4ckcUsfl5TfOWVj5wz4QVF6EGV7FxEnQHrdx0\n"
            + "6rXThRKFjqxUubsNt1yUEwdlTNz2UFhobGF9MmFeB97BZ6T4v8G825de/Caq9FzO\n"
            + "yMFFCRcGC7gIzMXRPEjHIvBdTThm9rbNzKPXHqw0LHG478yIqzxvraCYTRw/4eWN\n"
            + "Q+hyOL/5T5QNXHpR8Udp/7sptw7HfRnecQ/Vz9hOKShQq3h4Sz6eQMQm7P9qGo/N\n"
            + "bltEAIECRVcNYLN8LuEORfeecNcV3BX+4BBniFtdD2bIRsWC0ZUsGf14Yhr4P1OA\n"
            + "PtMJzy99mrcq3h+o+hEW6bhIj1gA88JSMJ4iRuwTLRKE81w7EyziScDsotYKvDPu\n"
            + "w4+PFbQO3fr/Zga3LgYis8/DMqZoWjVCjAeVoypuOZreieZYC/BgBS8qSUAmDPKq\n"
            + "jK+T5pwMMchfXbkV80LTu1kqLfKWdE0AmZfGy8COE/NNZ/FeiWZPdwu2Ix6u/RoY\n"
            + "LTjNy4YLIBdVELFXaFJF2GfzLpnwrW5tyNPVVrGmUoiyOzgx8gMyCLGavGtduyoY\n"
            + "tBiUTmd05Ugscn4Rz9X30S4NbnjL/h+bWl1m6/M+9FHEe85FPxmt/GRmJPbFPMR5\n"
            + "q5EgQGkt4ifiaP6qvyFulwvVwx+m0bf1q6Vb/k3clIyLMcVZWFE1TqNH2Ife46AE\n"
            + "2I39ZnGTt0mbWskpHBA=\n"
            + "-----END ENCRYPTED PRIVATE KEY-----";

    private static final Password KEY_PASSWORD = new Password("key-password");

    private DefaultSslEngineFactory factory = sslEngineFactory();
    Map<String, Object> configs = new HashMap<>();

    @BeforeEach
    public void setUp() {
        factory = sslEngineFactory();
        configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
    }

    protected DefaultSslEngineFactory sslEngineFactory() {
        return new DefaultSslEngineFactory();
    }

    @Test
    public void testPemTrustStoreConfigWithOneCert() throws Exception {
        configs.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, pemAsConfigValue(CA1));
        configs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        factory.configure(configs);

        KeyStore trustStore = factory.truststore();
        List<String> aliases = Collections.list(trustStore.aliases());
        assertEquals(Collections.singletonList("kafka0"), aliases);
        assertNotNull(trustStore.getCertificate("kafka0"), "Certificate not loaded");
        assertNull(trustStore.getKey("kafka0", null), "Unexpected private key");
    }

    @Test
    public void testPemTrustStoreConfigWithMultipleCerts() throws Exception {
        configs.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, pemAsConfigValue(CA1, CA2));
        configs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        factory.configure(configs);

        KeyStore trustStore = factory.truststore();
        List<String> aliases = Collections.list(trustStore.aliases());
        assertEquals(Arrays.asList("kafka0", "kafka1"), aliases);
        assertNotNull(trustStore.getCertificate("kafka0"), "Certificate not loaded");
        assertNull(trustStore.getKey("kafka0", null), "Unexpected private key");
        assertNotNull(trustStore.getCertificate("kafka1"), "Certificate not loaded");
        assertNull(trustStore.getKey("kafka1", null), "Unexpected private key");
    }

    @Test
    public void testPemKeyStoreConfigNoPassword() throws Exception {
        verifyPemKeyStoreConfig(KEY, null);
    }

    @Test
    public void testPemKeyStoreConfigWithKeyPassword() throws Exception {
        verifyPemKeyStoreConfig(ENCRYPTED_KEY, KEY_PASSWORD);
    }

    @Test
    public void testTrailingNewLines() throws Exception {
        verifyPemKeyStoreConfig(ENCRYPTED_KEY + "\n\n", KEY_PASSWORD);
    }

    @Test
    public void testLeadingNewLines() throws Exception {
        verifyPemKeyStoreConfig("\n\n" + ENCRYPTED_KEY, KEY_PASSWORD);
    }

    @Test
    public void testCarriageReturnLineFeed() throws Exception {
        verifyPemKeyStoreConfig(ENCRYPTED_KEY.replaceAll("\n", "\r\n"), KEY_PASSWORD);
    }

    private void verifyPemKeyStoreConfig(String keyFileName, Password keyPassword) throws Exception {
        configs.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, pemAsConfigValue(keyFileName));
        configs.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, pemAsConfigValue(CERTCHAIN));
        configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        configs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        factory.configure(configs);

        KeyStore keyStore = factory.keystore();
        List<String> aliases = Collections.list(keyStore.aliases());
        assertEquals(Collections.singletonList("kafka"), aliases);
        assertNotNull(keyStore.getCertificate("kafka"), "Certificate not loaded");
        assertNotNull(keyStore.getKey("kafka", keyPassword == null ? null : keyPassword.value().toCharArray()),
            "Private key not loaded");
    }

    @Test
    public void testPemTrustStoreFile() throws Exception {
        configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, pemFilePath(CA1));
        configs.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        factory.configure(configs);

        KeyStore trustStore = factory.truststore();
        List<String> aliases = Collections.list(trustStore.aliases());
        assertEquals(Collections.singletonList("kafka0"), aliases);
        assertNotNull(trustStore.getCertificate("kafka0"), "Certificate not found");
        assertNull(trustStore.getKey("kafka0", null), "Unexpected private key");
    }

    @Test
    public void testPemKeyStoreFileNoKeyPassword() throws Exception {
        configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                pemFilePath(pemAsConfigValue(KEY, CERTCHAIN).value()));
        configs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, null);
        factory.configure(configs);

        KeyStore keyStore = factory.keystore();
        List<String> aliases = Collections.list(keyStore.aliases());
        assertEquals(Collections.singletonList("kafka"), aliases);
        assertNotNull(keyStore.getCertificate("kafka"), "Certificate not loaded");
        assertNotNull(keyStore.getKey("kafka", null), "Private key not loaded");
    }

    @Test
    public void testPemKeyStoreFileWithKeyPassword() throws Exception {
        configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                pemFilePath(pemAsConfigValue(ENCRYPTED_KEY, CERTCHAIN).value()));
        configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, KEY_PASSWORD);
        configs.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, DefaultSslEngineFactory.PEM_TYPE);
        factory.configure(configs);

        KeyStore keyStore = factory.keystore();
        List<String> aliases = Collections.list(keyStore.aliases());
        assertEquals(Collections.singletonList("kafka"), aliases);
        assertNotNull(keyStore.getCertificate("kafka"), "Certificate not found");
        assertNotNull(keyStore.getKey("kafka", KEY_PASSWORD.value().toCharArray()), "Private key not found");
    }

    private String pemFilePath(String pem) throws Exception {
        return TestUtils.tempFile(pem).getAbsolutePath();
    }

    private Password pemAsConfigValue(String... pemValues) {
        StringBuilder builder = new StringBuilder();
        for (String pem : pemValues) {
            builder.append(pem);
            builder.append("\n");
        }
        return new Password(builder.toString().trim());
    }
}
