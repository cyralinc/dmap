package classifier_ssn_test

import data.classifier_ssn
import rego.v1

test_no_label if {
	classifier_ssn.output.column == false with input as {"column": "invalid"}
}

test_invalid_first_000 if {
	classifier_ssn.output.column == false with input as {"column": "000123456"}
}

test_invalid_first_666 if {
	classifier_ssn.output.column == false with input as {"column": "666123456"}
}

test_invalid_first_900s if {
	classifier_ssn.output.column == false with input as {"column": "905123456"}
}

test_invalid_middle if {
	classifier_ssn.output.column == false with input as {"column": "123006789"}
}

test_invalid_end if {
	classifier_ssn.output.column == false with input as {"column": "123450000"}
}

test_ssn_on_valid_amex_ccn if {
	classifier_ssn.output.message == false with input as {"message": "370136066365291"}
	classifier_ssn.output.message == false with input as {"message": "370136066365291"}
}

test_ssn_on_valid_visa_ccn if {
	classifier_ssn.output.message == false with input as {"message": "4613688275707134"}
}

test_ssn_on_valid_mastercard_format_1_ccn if {
	classifier_ssn.output.message == false with input as {"message": "5423909386888564"}
}

test_ssn_on_valid_mastercard_format_2_ccn if {
	classifier_ssn.output.message == false with input as {"message": "2701306282695666"}
}

test_ssn_on_valid_discover_ccn if {
	classifier_ssn.output.message == false with input as {"message": "6536673682309236"}
}

test_valid_ssn_dashes if {
	classifier_ssn.output.message == true with input as {"message": "111-11-1111"}
}

test_valid_ssn_dashes_sequential if {
	classifier_ssn.output.message == true with input as {"message": "123-45-6789"}
}

test_valid_ssn_dashes_in_value if {
	classifier_ssn.output.message == true with input as {"message": "this has a ssn 111-11-1111 that is valid"}
}

test_valid_ssn_no_dashes if {
	classifier_ssn.output.message == true with input as {"message": "111111111"}
}

test_valid_ssn_no_dashes_sequential if {
	classifier_ssn.output.message == true with input as {"message": "123456789"}
}

test_valid_ssn_no_dashes_in_value if {
	classifier_ssn.output.message == true with input as {"message": "this has a ssn 111111111 that is valid"}
}
