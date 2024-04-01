package classifier_last_name_test

import data.classifier_last_name
import rego.v1

test_ln_pattern if {
	classifier_last_name.output.last_name == true with input as {"last_name": "John"}
	classifier_last_name.output.last_name == true with input as {"last_name": "Robert"}
	classifier_last_name.output.lastname == true with input as {"lastname": "Robert"}
	classifier_last_name.output.lastName == true with input as {"lastName": "Robert"}
	classifier_last_name.output.Last_Name == true with input as {"Last_Name": "Robert"}
	classifier_last_name.output.LastName == true with input as {"LastName": "Robert"}
}

test_sn_pattern if {
	classifier_last_name.output.sur_name == true with input as {"sur_name": "John"}
	classifier_last_name.output.sur_name == true with input as {"sur_name": "Robert"}
	classifier_last_name.output.surname == true with input as {"surname": "Robert"}
	classifier_last_name.output.surName == true with input as {"surName": "Robert"}
	classifier_last_name.output.Sur_Name == true with input as {"Sur_Name": "Robert"}
	classifier_last_name.output.SurName == true with input as {"SurName": "Robert"}
}
