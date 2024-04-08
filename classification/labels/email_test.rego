package classifier_email_test

import data.classifier_email
import rego.v1

test_no_label if {
	classifier_email.output.column == false with input as {"column": "invalid"}
}

test_valid_email_com if {
	classifier_email.output.message == true with input as {"message": "me@me.com"}
}

test_valid_email_us if {
	classifier_email.output.message == true with input as {"message": "me@state.pa.us"}
}
