#!/bin/bash
#
#
# This scripts generates:
#  - root CA certificate
#  - server certificate and keystore
#  - client keys
#
# https://cwiki.apache.org/confluence/display/KAFKA/Deploying+SSL+for+Kafka
#


if [[ "$1" == "-k" ]]; then
    USE_KEYTOOL=1
    shift
else
    USE_KEYTOOL=0
fi

OP="$1"
CA_CERT="$2"
PFX="$3"
HOST="$4"

C=NN
ST=NN
L=NN
O=NN
OU=NN
CN="$HOST"
 

# Password
PASS="abcdefgh"

# Cert validity, in days
VALIDITY=10000

set -e

export LC_ALL=C

if [[ $OP == "ca" && ! -z "$CA_CERT" && ! -z "$3" ]]; then
    CN="$3"
    openssl req -new -x509 -keyout ${CA_CERT}.key -out $CA_CERT -days $VALIDITY -passin "pass:$PASS" -passout "pass:$PASS" <<EOF
${C}
${ST}
${L}
${O}
${OU}
${CN}
$USER@${CN}
.
.
EOF



elif [[ $OP == "server" && ! -z "$CA_CERT" && ! -z "$PFX" && ! -z "$CN" ]]; then

    #Step 1
    echo "############ Generating key"
    keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}server.keystore.jks -alias localhost -validity $VALIDITY -genkey -keyalg RSA <<EOF
$CN
$OU
$O
$L
$ST
$C
yes
yes
EOF
	
    #Step 2
    echo "############ Adding CA"
    keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}server.truststore.jks -alias CARoot -import -file $CA_CERT <<EOF
yes
EOF
    
    #Step 3
    echo "############ Export certificate"
    keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}server.keystore.jks -alias localhost -certreq -file ${PFX}cert-file

    echo "############ Sign certificate"
    openssl x509 -req -CA $CA_CERT -CAkey ${CA_CERT}.key -in ${PFX}cert-file -out ${PFX}cert-signed -days $VALIDITY -CAcreateserial -passin "pass:$PASS"
    
    
    echo "############ Import CA"
    keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}server.keystore.jks -alias CARoot -import -file $CA_CERT <<EOF
yes
EOF
    
    echo "############ Import signed CA"
    keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}server.keystore.jks -alias localhost -import -file ${PFX}cert-signed    

    
elif [[ $OP == "client" && ! -z "$CA_CERT" && ! -z "$PFX" && ! -z "$CN" ]]; then

    if [[ $USE_KEYTOOL == 1 ]]; then
	echo "############ Creating client truststore"

	[[ -f ${PFX}client.truststore.jks ]] || keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}client.truststore.jks -alias CARoot -import -file $CA_CERT <<EOF
yes
EOF

	echo "############ Generating key"
	keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}client.keystore.jks -alias localhost -validity $VALIDITY -genkey -keyalg RSA <<EOF
$CN
$OU
$O
$L
$ST
$C
yes
yes
EOF
	echo "########### Export certificate"
	keytool -storepass "$PASS" -keystore ${PFX}client.keystore.jks -alias localhost -certreq -file ${PFX}cert-file

	echo "########### Sign certificate"
	openssl x509 -req -CA ${CA_CERT} -CAkey ${CA_CERT}.key -in ${PFX}cert-file -out ${PFX}cert-signed -days $VALIDITY -CAcreateserial -passin pass:$PASS	

	echo "########### Import CA"
	keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}client.keystore.jks -alias CARoot -import -file ${CA_CERT} <<EOF
yes
EOF

	echo "########### Import signed CA"
	keytool -storepass "$PASS" -keypass "$PASS" -keystore ${PFX}client.keystore.jks -alias localhost -import -file ${PFX}cert-signed

    else
	# Standard OpenSSL keys
	echo "############ Generating key"
	openssl genrsa -des3 -passout "pass:$PASS" -out ${PFX}client.key 2048 
	
	echo "############ Generating request"
	openssl req -passin "pass:$PASS" -passout "pass:$PASS" -key ${PFX}client.key -new -out ${PFX}client.req \
		<<EOF
$C
$ST
$L
$O
$OU
$CN
.
$PASS
.
EOF

	echo "########### Signing key"
	openssl x509 -req -passin "pass:$PASS" -in ${PFX}client.req -CA $CA_CERT -CAkey ${CA_CERT}.key -CAcreateserial -out ${PFX}client.pem -days $VALIDITY

    fi

    
    

else
    echo "Usage: $0 ca <ca-cert-file> <CN>"
    echo "       $0 [-k] server|client <ca-cert-file> <file_prefix> <hostname>"
    echo ""
    echo "       -k = Use keytool/Java Keystore, else standard SSL keys"
    exit 1
fi