package classifier_phone_test

import data.classifier_phone
import rego.v1

test_no_label if {
	classifier_phone.output.column == false with input as {"column": "invalid"}
}

test_valid_phone if {
	classifier_phone.output.message == true with input as {"message": "7175551212"}
}

test_valid_us_phone if {
	classifier_phone.output.message == true with input as {"message": "17175551212"}
}
