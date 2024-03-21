import json
import frappe
import bulkwebhook
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)

from bulkwebhook.bulk_webhook.doctype.bulk_webhook.bulk_webhook import log_request
from bulkwebhook.bulk_webhook.doctype.kafka_request_log.kafka_request_log import (
    create_kafka_request_log,
)

"""
    Confluent Kafka libray is currently used to send Protobuf data to Kafka. 
    This library was chosen because the previous library (Kafka-Python) used to send Protobuf data to Kafka,
    was not meet the Protobuf Serialization requirements required by the kafka consumers.
    
    March 16th, 2023
"""


def get_confluent_kafka_client(settings_doc):
    """
    Create a Confluent_kafka Producer instance for the given settings name.

    args:
        settings_doc: Kafka Settings document
    """

    conf = {
        "bootstrap.servers": settings_doc.bootstrap_servers,
        "client.id": settings_doc.client_id,
        "security.protocol": settings_doc.security_protocol,
        "sasl.mechanism": "PLAIN",
        "sasl.username": settings_doc.get_password("api_key"),
        "sasl.password": settings_doc.get_password("api_secret"),
    }

    return Producer(**conf)


def get_confluent_kafka_producer(settings_doc) -> Producer:
    """
    Return a ConfluentKafkaProducer instance for the given settings name.
    If the producer is already created, return the same instance.
    Otherwise, create a new instance and return it.

    args:
        settings_doc: Kafka Settings document
    """
    if frappe.local.site not in bulkwebhook.PRODUCER_MAP:
        bulkwebhook.PRODUCER_MAP[frappe.local.site] = {}

    if (
        f"{settings_doc.name}_confluent_kafka_producer"
        not in bulkwebhook.PRODUCER_MAP[frappe.local.site]
    ):
        bulkwebhook.PRODUCER_MAP[frappe.local.site][
            f"{settings_doc.name}_confluent_kafka_producer"
        ] = get_confluent_kafka_client(settings_doc)

    return bulkwebhook.PRODUCER_MAP[frappe.local.site][
        f"{settings_doc.name}_confluent_kafka_producer"
    ]


def get_schema_registry_client(settings_doc):
    """
    Return a SchemaRegistryClient instance for the given settings name.
    If the client is already created, return the same instance.
    Otherwise, create a new instance and return it.

    args:
        setting_doc: Kafka Settings document
    """
    if frappe.local.site not in bulkwebhook.PRODUCER_MAP:
        bulkwebhook.PRODUCER_MAP[frappe.local.site] = {}

    if (
        f"{settings_doc.name}_schema_registry_client"
        in bulkwebhook.PRODUCER_MAP[frappe.local.site]
    ):
        return bulkwebhook.PRODUCER_MAP[frappe.local.site][
            f"{settings_doc.name}_schema_registry_client"
        ]

    else:
        schema_registry_client = SchemaRegistryClient(
            {
                "url": f"{settings_doc.schema_regestry_url}",
                "basic.auth.user.info": f"{settings_doc.username}:{settings_doc.get_password('password')}",
            }
        )
        if schema_registry_client:
            bulkwebhook.PRODUCER_MAP[frappe.local.site][
                f"{settings_doc.name}_schema_registry_client"
            ] = schema_registry_client
            return bulkwebhook.PRODUCER_MAP[frappe.local.site][
                f"{settings_doc.name}_schema_registry_client"
            ]
        else:
            frappe.log_error(
                "Schema Registry Client not found, Check the schema registry configuration settings",
                title="Schema Registry Client Error",
            )
            frappe.throw(
                "Schema Registry Client not found, Please check the schema registry configuration settings"
            )


def run_kafka_hook_for_protobuf(kafka_hook, doctype, doc=None, doc_list=None):
    """
    Run kafka hook for protobuf data

    args:
        kafka_hook: Kafka Hook document
        doctype: Doctype name
        doc: Document object
        doc_list: List of document names
    """

    proto_obj = None
    data_list = []

    if doc:
        data = frappe.get_attr(kafka_hook.webhook_method)(doc)
        data_list.append(data.get("data"))
        if not proto_obj:
            proto_obj = data.get("proto_obj")
        create_kafka_request_log(doctype=kafka_hook.webhook_doctype, docname=doc.name, status="sending to kafka")

    elif doc_list:
        for record in doc_list:
            doc = frappe.get_doc(doctype, record)
            data = frappe.get_attr(kafka_hook.webhook_method)(doc)
            data_list.append(data.get("data"))
            if not proto_obj:
                proto_obj = data.get("proto_obj")
        create_kafka_request_log(doctype=kafka_hook.webhook_doctype, status="sending to kafka", doc_list=doc_list)

    settings_doc = frappe.get_cached_doc("Kafka Settings", kafka_hook.kafka_settings)
    if not settings_doc.security_protocol:
        frappe.throw(
            "Security Protocol is required for Kafka Settings, please set it on the Kafka Settings document."
        )
    schema_registry_client = get_schema_registry_client(settings_doc)
    producer = get_confluent_kafka_producer(settings_doc)

    send_protobuf_data(
        producer, schema_registry_client, kafka_hook, data_list, proto_obj
    )


def send_protobuf_data(
    producer, schema_registry_client, kafka_hook, data_list, proto_obj
):
    """Send serialized protobuf data to kafka

    args:
        producer: Confluent_kafka producer object
        topic: name of the topic
        value: Data to be sent to kafka
        key: the id used for kafka
        proto_obj: _pb2 object
    """

    serializer_conf = {
        "auto.register.schemas": True,
        "normalize.schemas": True,
        "use.latest.version": False,
        "use.deprecated.format": False,
    }
    protobuf_serializer = ProtobufSerializer(
        proto_obj, schema_registry_client, serializer_conf
    )
    count = 0
    try:
        for value in data_list:
            producer.poll(0.0)
            producer.produce(
                topic=kafka_hook.kafka_topic,
                key=value.id,
                value=protobuf_serializer(
                    value,
                    SerializationContext(kafka_hook.kafka_topic, MessageField.VALUE),
                )
                # on_delivery=callback_response
            )
            count += 1
    except Exception as e:
        raise e

    producer.flush()

    data_sent = None
    if count > 1:
        data_sent = f"Bulk Protobuf Data sent to Kafka: {count}"
    elif count == 1:
        data_sent = data_list[0]

    create_kafka_request_log(doctype=kafka_hook.webhook_doctype, docname=data_list[0].name, status="Sent to kafka",  doc_list=data_list)
    log_request(kafka_hook.kafka_topic, kafka_hook.kafka_settings, data_sent, "")


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
            "partition": str(msg.partition()),
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
