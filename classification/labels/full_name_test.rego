package classifier_full_name_test

import data.classifier_full_name
import rego.v1

test_fn_pattern if {
	classifier_full_name.output.full_name == true with input as {"full_name": "John"}
	classifier_full_name.output.full_name == true with input as {"full_name": "Robert"}
	classifier_full_name.output.fullname == true with input as {"fullname": "Robert"}
	classifier_full_name.output.fullName == true with input as {"fullName": "Robert"}
	classifier_full_name.output.Full_Name == true with input as {"Full_Name": "Robert"}
	classifier_full_name.output.FullName == true with input as {"FullName": "Robert"}
}
