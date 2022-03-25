#!/bin/bash

DATAWAVE_ENDPOINT=https://localhost:8443/query/v1
METRICS_ENDPOINT=https://localhost:8543/querymetric/v1

PAUSE='false'

POOL="${POOL:-pool1}"

MAX_PAGES=100

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# use the test user pkcs12 cert
P12_KEYSTORE=${SCRIPT_DIR}/../pki/testUser.p12
P12_KEYSTORE_PASS=ChangeIt

TMP_DIR=/dev/shm
TMP_PEM="$TMP_DIR/testUser-$$-pem"

sh -c "while kill -0 $$ 2>/dev/null; do sleep 1; done; rm -f '${TMP_P12}' '${TMP_PEM}'" &

function needsPassphrase() {
    [ -z "${P12_KEYSTORE_PASS}" ]
}

function getFromCliPrompt() {
    read -s -p "Passphrase for ${P12_KEYSTORE}: " P12_KEYSTORE_PASS && echo 1>&2
}

needsPassphrase && getFromCliPrompt

# Create one-time passphrase and certificate
OLD_UMASK=$(umask)
umask 0277
export P12_KEYSTORE_PASS
openssl pkcs12 \
    -in ${P12_KEYSTORE} -passin env:P12_KEYSTORE_PASS \
    -out ${TMP_PEM} -nodes 2>/dev/null
opensslexit=$?
umask $OLD_UMASK
[ $opensslexit = 0 ] || errormsg "Error creating temporary certificate file"

read_dom () {
    local IFS=\>
    read -d \< ENTITY CONTENT
}

get_query_id () {
    while read_dom; do
        if [[ $ENTITY =~ 'Result' ]] && [[ ! $ENTITY =~ 'HasResults'  ]]; then
            echo $CONTENT
            break
        fi
    done
}

get_num_events () {
    while read_dom; do
        if [[ $ENTITY = 'ReturnedEvents' ]]; then
            echo $CONTENT
            break
        fi
    done
}

FOLDER="errorCount_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

SYSTEM_FROM=$(hostname)

echo "$(date): Creating query"
echo "$(date): Creating query" > querySummary.txt
curl -s -D headers_0.txt -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    --data-urlencode "begin=19660908 000000.000" \
    --data-urlencode "end=20301231 235959.999" \
    --data-urlencode "columnVisibility=PUBLIC" \
    --data-urlencode "query=FOO_FIELD:myFoo" \
    --data-urlencode "query.syntax=LUCENE" \
    --data-urlencode "auths=PUBLIC,PRIVATE,BAR,FOO" \
    --data-urlencode "systemFrom=$SYSTEM_FROM" \
    --data-urlencode "queryName=Developer Test Query" \
    --data-urlencode "pagesize=10" \
    --data-urlencode "pool=$POOL" \
    ${DATAWAVE_ENDPOINT}/ErrorCountQuery/create -o createResponse.xml -w '%{http_code}\n' >> querySummary.txt

i=1

QUERY_ID=$(get_query_id < createResponse.xml)

while [ $i -gt 0 ] && [ $i -lt $MAX_PAGES ]; do
    if [ "$PAUSE" == "true" ]; then
        echo "press any key to continue"
        read -n 1
    fi

    echo "$(date): Requesting page $i for $QUERY_ID"
    echo "$(date): Requesting page $i for $QUERY_ID" >> querySummary.txt
    curl -s -D headers_$i.txt -q -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        ${DATAWAVE_ENDPOINT}/$QUERY_ID/next -o nextResponse_$i.xml -w '%{http_code}\n' >> querySummary.txt

    CONTINUE=`grep 'HTTP/1.1 200 OK' headers_$i.txt`

    if [ -z "$CONTINUE" ]; then
        i=-1
    else
        NUM_EVENTS=$(get_num_events < nextResponse_$i.xml)
        echo "$(date): Page $i contained $NUM_EVENTS events"
        echo "$(date): Page $i contained $NUM_EVENTS events" >> querySummary.txt

        ((i++))
    fi
done

echo "$(date): Closing $QUERY_ID"
echo "$(date): Closing $QUERY_ID" >> querySummary.txt
# close the query
curl -s -q -k -X POST -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    ${DATAWAVE_ENDPOINT}/$QUERY_ID/close -o closeResponse.xml -w '%{http_code}\n' >> querySummary.txt

cd ../

if [ ! -z "$QUERY_ID" ]; then
    mv $FOLDER errorCount_$QUERY_ID

    echo "$(date): Getting metrics for $QUERY_ID"
    echo "$(date): Getting metrics for $QUERY_ID" >> errorCount_$QUERY_ID/querySummary.txt

    echo "$(date): Metrics available at: ${METRICS_ENDPOINT}/id/$QUERY_ID"
    echo "$(date): Metrics available at: ${METRICS_ENDPOINT}/id/$QUERY_ID" >> errorCount_$QUERY_ID/querySummary.txt
fi