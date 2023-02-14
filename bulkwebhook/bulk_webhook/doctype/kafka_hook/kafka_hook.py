# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import json
from typing import Dict
import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.jinja import validate_template
from bulkwebhook.bulk_webhook.doctype.kafka_settings.kafka_utlis import send_kafka
from bulkwebhook.bulk_webhook.doctype.bulk_webhook.bulk_webhook import log_request


def get_safe_frappe_utils():
    from frappe.utils.safe_exec import add_data_utils

    data_utils = frappe._dict()
    add_data_utils(data_utils)
    return data_utils


WEBHOOK_CONTEXT = {"utils": get_safe_frappe_utils()}


class KafkaHook(Document):
    def validate(self):
        self.validate_docevent()
        self.validate_condition()
        self.validate_request_body()

    def on_update(self):
        frappe.cache().delete_value("kafkahook")

    def on_trash(self):
        frappe.cache().delete_value("kafkahook")

    def validate_docevent(self):
        if self.webhook_doctype:
            is_submittable = frappe.get_value(
                "DocType", self.webhook_doctype, "is_submittable"
            )
            if not is_submittable and self.webhook_docevent in [
                "on_submit",
                "on_cancel",
                "on_update_after_submit",
            ]:
                frappe.throw(
                    _("DocType must be Submittable for the selected Doc Event")
                )

    def validate_condition(self):
        temp_doc = frappe.new_doc(self.webhook_doctype)
        if self.condition:
            try:
                frappe.safe_eval(
                    self.condition, eval_locals={**WEBHOOK_CONTEXT, "doc": temp_doc}
                )
            except Exception as e:
                frappe.throw(_(e))

    def validate_request_body(self):
        validate_template(self.webhook_json)


def run_kafka_hook(
    kafka_hook_name: str,
    doc=None,
    doctype=None,
    doc_list=None,
):
    hook: KafkaHook = frappe.get_cached_doc("Kafka Hook", kafka_hook_name)
    is_from_request = bool(frappe.request)

    if doc:
        _run_kafka_hook(hook, doc)
        return

    if isinstance(doc_list, str):
        doc_list = [doc_list]

    for doc_name in set(doc_list):
        try:
            doc = frappe.get_doc(doctype, doc_name)
            _run_kafka_hook(hook, doc)
        except Exception:
            if is_from_request:
                raise

            frappe.log_error(title="Error running Kafka Hook")


def _run_kafka_hook(hook, doc):
    data = get_webhook_data(doc, hook)

    try:
        r = send_kafka(
            hook.kafka_settings,
            hook.kafka_topic,
            None,
            data,
        )
        log_request(hook.kafka_topic, hook.kafka_settings, data, str(r))

    except Exception as e:
        frappe.log_error(str(e))
        log_request(
            "Error: " + hook.kafka_topic,
            hook.kafka_settings,
            data,
            r,
        )


def get_webhook_data(doc: Document, kafka_hook: KafkaHook) -> dict:
    """Returns webhook data (generated from KafkaHook.webhook_json) for the given document and webhook"""
    data = {}
    doc = doc.as_dict(convert_dates_to_str=True)
    data = frappe.render_template(
        kafka_hook.webhook_json, context={**WEBHOOK_CONTEXT, "doc": doc}
    )
    data = json.loads(data)
    return data


def enqueue_kafka_hook(doc: Document, webhook: frappe._dict):
    frappe.enqueue(
        "bulkwebhook.bulk_webhook.doctype.kafka_hook.kafka_hook.run_kafka_hook",
        enqueue_after_commit=True,
        doc=doc,
        kafka_hook=webhook,
    )

    # keep list of webhooks executed for this doc in this request
    # so that we don't run the same webhook for the same document multiple times
    # in one request
    frappe.flags.kafkahook_executed.setdefault(doc.name, []).append(webhook.name)


def generate_kafkahook() -> Dict[str, list]:
    webhooks = {}
    webhooks_list = frappe.get_all(
        "Kafka Hook",
        fields=["name", "`condition`", "webhook_docevent", "webhook_doctype"],
        filters={"enabled": True},
    )
    for w in webhooks_list:
        webhooks.setdefault(w.webhook_doctype, []).append(w)
    return webhooks


def fetch_webhooks_from_redis() -> dict:
    return frappe.cache().get_value("kafkahook", generator=generate_kafkahook)


def run_webhooks(doc: Document, method: str):
    """Run webhooks for this method"""
    if (
        frappe.flags.in_import
        or frappe.flags.in_patch
        or frappe.flags.in_install
        or frappe.flags.in_migrate
    ):
        return

    event_list = ["on_update", "after_insert", "on_submit", "on_cancel", "on_trash"]
    # value change is not applicable in insert
    if not doc.flags.in_insert:
        event_list.extend(["on_change", "before_update_after_submit"])

    # skip if method is not in applicable event list
    if method not in event_list:
        return

    if frappe.flags.kafkahook_executed is None:
        frappe.flags.kafkahook_executed = {}

    if frappe.flags.kafkahook is None:
        frappe.flags.kafkahook = fetch_webhooks_from_redis()

    webhooks_for_doc = frappe.flags.kafkahook.get(doc.doctype)

    if not webhooks_for_doc:
        return

    for webhook in webhooks_for_doc:
        if webhook.webhook_docevent != method:
            continue

        # skip if webhook already executed for this doc in this request
        if webhook.name in frappe.flags.kafkahook_executed.get(doc.name, []):
            continue

        # skip if webhook condition is not fulfilled
        trigger_webhook = False
        if not webhook.condition:
            trigger_webhook = True
        elif frappe.safe_eval(
            webhook.condition, eval_locals={**WEBHOOK_CONTEXT, "doc": doc}
        ):
            trigger_webhook = True
        if not trigger_webhook:
            continue

        enqueue_kafka_hook(doc, webhook)
