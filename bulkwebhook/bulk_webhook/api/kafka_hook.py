# Copyright (c) 2022, Aakvatech and contributors
# For license information, please see license.txt

import frappe
import json
from frappe import _
from frappe.utils.background_jobs import enqueue
from bulkwebhook.bulk_webhook.doctype.kafka_hook.kafka_hook import run_kafka_hook


@frappe.whitelist()
def resend_single_kafkahook(doctype, doc_name, kafkahook_name=None):
    """Resend a Kafka Hook for a single document
    API Path: <URL>://bulkwebhook.bulk_webhook.api.kafka_hook.resend_single_kafkahook

    Parameters:
    doctype: Data - Document Type Name to fire the Kafka Hook on. e.g. "Sales Order"
    doc_name: Data - Name of document for which to fire the Kafka Hook. e.g. "SAL-ORD-JUJ002"
    kafkahook_name (Optional): Document Name of the Kafka Hook to fire. e.g. "HOOK-0016"

    Returns:
    dict: {"success": True, "message": "Kafka Hook fired successfully"}
    """

    if not kafkahook_name:
        kafkahook_name = frappe.get_value(
            "Kafka Hook",
            {"webhook_doctype": doctype, "enabled": 1, "condition": ""},
            "name",
        )

    if not kafkahook_name:
        frappe.throw(
            _("Please set a webhook in the Setup > Webhooks with blank condition")
        )

    run_kafka_hook(kafkahook_name, doctype=doctype, doc_list=[doc_name])
    frappe.msgprint("Webhook sent successfully")


@frappe.whitelist()
def resend_kafkahook_for_docs(args):
    """API to send webhook for a list of documents
    API Path: <URL>://bulkwebhook.bulk_webhook.api.kafka_hook.resend_kafkahook_for_docs
    args includes:
    kafkahook_name: Document Name of the kafkahook to fire. e.g. "HOOK-0016"
    doctype_name: Document Type Name to fire the kafkahook on. e.g. "Sales Order"
    doc_list: list - List of Names of documents for which to fire the kafkahook. e.g. ["SAL-ORD-JUJ001", "SAL-ORD-JUJ002"]
    """

    kafkahook_name = args.get("kafkahook_name")
    if not kafkahook_name:
        frappe.throw("Webhook Name is required")

    doctype_name = args.get("doctype_name")
    if not doctype_name:
        frappe.throw("Doctype Name is required")

    doc_list = frappe.parse_json(args.get("doc_list"))
    if not doc_list:
        frappe.throw("Doc List is required")

    enqueue(
        method=run_kafka_hook,
        queue="long",
        timeout=24000,
        is_async=True,
        kafka_hook_name=kafkahook_name,
        doctype=doctype_name,
        doc_list=doc_list,
    )

    return "Webhook sending is schdeduled"
