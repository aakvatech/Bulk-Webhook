import json

import frappe
import bulkwebhook
from kafka import KafkaProducer
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField



"""
    Confluent Kafka libray is currently used to send Protobuf data to Kafka. 
    This library was chosen because the previous library (Kafka Python) used to send Protobuf data to Kafka,
    was not meet the Protobuf Serialization requirements required by the kafka consumers.
    
    March 16th, 2023
"""

def get_kafka_client(settings, method=None):
    """Create a KafkaProducer or Confluent_kafka Producer instance for the given settings name."""

    if method:
        # create a confluent_kafka producer instance
        conf = {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": settings.client_id,
            "security.protocol":"SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": settings.get_password("api_key"),
            "sasl.password": settings.get_password("api_secret"),
        }
        
        return Producer(**conf)
    
    # create a KafkaProducer instance
    return KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        value_serializer=lambda e: serialize_data(e),
        key_serializer=lambda e: serialize_data(e),
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=settings.get_password("api_key"),
        sasl_plain_password=settings.get_password("api_secret"),
    )


def get_kafka_producer(settings, method=None) -> KafkaProducer: 
    """Return a KafkaProducer instance for the given settings name. If the producer is already
    created, return the same instance. Otherwise, create a new instance and return it.
    """
    if frappe.local.site not in bulkwebhook.PRODUCER_MAP:
        bulkwebhook.PRODUCER_MAP[frappe.local.site] = {}

    if settings.name not in bulkwebhook.PRODUCER_MAP[frappe.local.site]:
        bulkwebhook.PRODUCER_MAP[frappe.local.site][settings.name] = get_kafka_client(
            settings, method
        )

    return bulkwebhook.PRODUCER_MAP[frappe.local.site][settings.name]


def send_kafka(settings_name, topic, key, value, proto_obj=None, method=None):
    setting_doc = frappe.get_cached_doc("Kafka Settings", settings_name)
    producer = get_kafka_producer(setting_doc, method)
    if not method:
        future = (
            producer.send(topic=topic, key=key, value=value)
            .add_callback(on_send_success)
            .add_errback(on_send_error)
        )
        res = future.get(timeout=120)
        return res
    
    send_protobuf_data(producer, setting_doc, topic, value, key, proto_obj)


# NOTE: The on_send_success function is not working.
def on_send_success(record_metadata):
    frappe.log_error(
        str(
            {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
        )
    )


# NOTE: the on_send_error function is not working.
def on_send_error(excp):
    frappe.log_error(str(excp))
    # handle exception


# # produce asynchronously with callbacks
# producer.send("my-topic", b"raw_bytes").add_callback(on_send_success).add_errback(
#     on_send_error
# )



def serialize_data(data):
    """Serialize data to be sent to Kafka"""
    serialized_data = None
    try:
        serialized_data = json.dumps(data).encode("ascii")
    except TypeError:
        try:
            serialized_data = data.SerializeToString()

        except Exception as e:
            frappe.log_error(str(e), frappe.get_traceback())
            frappe.throw(str(e))

    return serialized_data


def send_protobuf_data(producer, setting_doc, topic, value, key, proto_obj):
    """Send serialized protobuf data to kafka
    
    params: producer: Confluent_kafka producer object
            setting_doc: Docement that have configuration properties
            topic: name of the topic
            value: Data to be sent to kafka
            key: the id used for kafka
            proto_obj: _pb2 object    
    """
    
    url = setting_doc.schema_regestry_url
    usr = setting_doc.username
    pwd = setting_doc.get_password("password")

    schema_registry_client = SchemaRegistryClient({
        "url": f"{url}",
        "basic.auth.user.info": f"{usr}:{pwd}"
    })

    string_serializer = StringSerializer('utf8')

    serializer_conf = {
        "auto.register.schemas": True,
        "normalize.schemas": True,
        "use.latest.version": False,
        "use.deprecated.format": False
    }
    protobuf_serializer = ProtobufSerializer(
            proto_obj,
            schema_registry_client, 
            serializer_conf
        )
    
    while True:
        try:
            producer.poll(0.0)
            producer.produce(
                topic=topic,
                key=string_serializer(str(key)),
                value=protobuf_serializer(value, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=callback_response
            )
            resp = producer.flush()
            return resp
        except Exception as e:
            raise e


def callback_response(err, msg):
    """collback method to register response from kafka"""
    
    if err is not None:
        frappe.log_error(frappe.get_traceback(), str(err))
        frappe.throw(str(err))
    else:
        resp = {
            "headers": str(msg.headers()),
            "key": str(msg.key()),
            "value": str(msg.value()),
            "offset": msg.offset(),
            "partition": str(msg.partition())
        }
        request_log = frappe.get_doc(
            {
                "doctype": "Webhook Request Log",
                "user": frappe.session.user if frappe.session.user else None,
                "url": None,
                "headers": None,
                "data": None,
                "response": json.dumps(resp, indent=4) if resp else None,
            }
        )

        request_log.insert(ignore_permissions=True)


        