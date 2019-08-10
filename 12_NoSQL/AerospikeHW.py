from __future__ import print_function
import aerospike
import sys
from aerospike import exception as ex
from aerospike import predicates as p
import logging
import time

def add_customer(customer_id, phone_number, lifetime_value):
    key = (namespace, customerSet, customer_id)
    bins = {'phone_number': phone_number, 'lifetime_value' : lifetime_value}
    client.put(key, bins, meta={'ttl':60})

def get_ltv_by_id(customer_id):
    try:
        key = (namespace, customerSet, customer_id)
        (key, meta, bins) = client.get(key)
        return bins.get("lifetime_value")
    except ex.RecordNotFound:
        logging.error('Requested non-existent customer ' + str(customer_id))

def get_ltv_by_phone_scan(phone_number):
    scan = client.scan(namespace, customerSet)
    scan.select('phone_number','lifetime_value')
    records = scan.results()
    for record in records:
        (key, meta, bins) = record
        if (bins.get("phone_number") == phone_number):
            return bins.get("lifetime_value")
    logging.error('Requested phone number is not found ' + str(phone_number))

def get_ltv_by_phone_query(phone_number):
    query = client.query(namespace,customerSet)
    query.where(p.equals("phone_number", phone_number))
    query.select("lifetime_value")
    for record in query.results():
        (key, meta, bins) = record
        return bins.get("lifetime_value")
    logging.error('Requested phone number is not found ' + str(phone_number))

def keyToPhoneNumber(key):
    return "+7495"+str(key)

def createIndex():
    try:
        start_time = time.time()
        client.index_string_create(namespace, customerSet, "phone_number", "ix_phone_number")
        print("Create secondary index. Elapsed time: "+str(time.time() - start_time))
    except ex.IndexError as e:
        logging.error("createIndex error " + e.msg)

def clearSet():
    try:
        client.truncate(namespace, "truncate", 0)
        client.index_remove(namespace, "ix_phone_number")
    except ex.AerospikeError as e:
        logging.error("clearSet error " + e.msg)

try:
    config = {
        'hosts': [ ('127.0.0.1', 3000) ]
    }
    client = aerospike.client(config).connect()
    
    namespace = "test"
    customerSet = "customer"

    clearSet();
    createIndex();

    #Add records
    start_time = time.time()
    for x in range(1000): 
        add_customer(x,keyToPhoneNumber(x), x+100)
    print("Add customers. Elapsed time: "+str(time.time() - start_time))
    
    #Search by key
    start_time = time.time()
    queryResult=""
    for x in range(1000): 
        queryResult+=str(get_ltv_by_id(x))+", "
    print("Search by key. Elapsed time: "+str(time.time() - start_time))
    print(queryResult[0:100]+"...")

    #Scan by phone number
    start_time = time.time()
    queryResult=""
    for x in range(100):
        phone_number = keyToPhoneNumber(x)
        queryResult+=phone_number+" -> "+str(get_ltv_by_phone_scan(phone_number))+", "
    print("Scan by phone number. Elapsed time: "+str(time.time() - start_time))
    print(queryResult[0:100]+"...")

    #Query by phone number
    start_time = time.time()
    queryResult=""
    for x in range(100):
        phone_number = keyToPhoneNumber(x)
        queryResult+=phone_number+" -> "+str(get_ltv_by_phone_query(phone_number))+", "
    print("Query by phone number(with index). Elapsed time: "+str(time.time() - start_time))
    print(queryResult[0:100]+"...")
    
    #Search not avaible records
    print("get customer_id=n/a ltv")
    print(get_ltv_by_id("n/a"))
    print("get customer_phone=n/a ltv")
    print(get_ltv_by_phone_scan("n/a"))
    
except Exception as e:
    print("error: {0}".format(e), file=sys.stderr)
    sys.exit(1)
finally:
    client.close()








