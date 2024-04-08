package classifier_ccn_test

import data.classifier_ccn
import rego.v1

test_no_label if {
	classifier_ccn.output.column == false with input as {"column": "invalid"}
}

test_valid_amex_ccn if {
	classifier_ccn.output.message == true with input as {"message": "370136066365291"}
}

test_valid_visa_ccn if {
	classifier_ccn.output.message == true with input as {"message": "4613688275707134"}
}

test_valid_mastercard_ccn if {
	classifier_ccn.output.message == true with input as {"message": "5423909386888564"}
	classifier_ccn.output.message == true with input as {"message": "2701306282695666"}
}

test_valid_discover_ccn if {
	classifier_ccn.output.message == true with input as {"message": "6536673682309236"}
}
