# Copyright (c) 2023, Aakvatech and contributors
# For license information, please see license.txt


import frappe
from frappe.model.document import Document
from frappe import _
import subprocess
import os
import re


class ProtoBufFile(Document):
	def validate(self):
		self.generate_protobuf_pythone_file()


	def generate_protobuf_pythone_file(self):
		protobuf_file_value = self.pr_file
		if not protobuf_file_value:
			frappe.throw(_("Please set protobuf file"))
		# make protobuf_file_value as bytes
		protobuf_file_value = self.pr_file.encode('utf-8')
		# use self.title as file name and remove unwanted characters and replace spaces with _ using regix
		temp_file_name  = re.sub('[^A-Za-z0-9]+', '_', self.title)
		# save protobuf file in temp directory in temp file
		temp_file_path = temp_file_name + ".proto"
		
		with open("/tmp/" + temp_file_path, 'wb') as f:
			f.write(protobuf_file_value)

		# generate python file from protobuf file and save in temp directory in temp file
		# prepare empty python file
		python_file_path = frappe.get_site_path("/tmp/" + temp_file_name + "_pb2.py")
		# if error in subprocess.run then throw error
		proto_process = subprocess.run(f"protoc --proto_path=/tmp  --experimental_allow_proto3_optional --python_out=/tmp {temp_file_path}", shell=True
			, stdout=subprocess.PIPE, stderr=subprocess.PIPE
		)
		# if error in subprocess.run then throw error
		if proto_process.returncode != 0:
			frappe.throw(proto_process.stderr.decode('utf-8'))
		
		# read python file and save in protobuf file
		with open(python_file_path, 'r') as f:
			python_file_value = f.read()
			# save python file in self.py_file
			self.py_file = python_file_value

		# delete temp files
		os.remove("/tmp/" + temp_file_path)
		os.remove(python_file_path)
