package classifier_address_test

import data.classifier_address
import rego.v1

test_no_label if {
	classifier_address.output.column == false with input as {"column": "invalid"}
}

test_column_name_state if {
	classifier_address.output.state == true with input as {"state": "AZ"}
}

test_insensitive_column_name_state if {
	classifier_address.output.STATE == true with input as {"STATE": "AZ"}
}

test_column_name_address if {
	classifier_address.output.address == true with input as {"address": "123 some street"}
}

test_insensitive_column_name_address if {
	classifier_address.output.ADDRESS == true with input as {"ADDRESS": "123 some street"}
}

test_column_name_contains_address if {
	classifier_address.output.some_address_here == true with input as {"some_address_here": "123 some street"}
}

test_insensitive_column_name_contains_address if {
	classifier_address.output.some_ADDRESS_Here == true with input as {"some_ADDRESS_Here": "123 some street"}
}

test_column_name_street if {
	classifier_address.output.street == true with input as {"street": "123 some street"}
}

test_insensitive_column_name_street if {
	classifier_address.output.STREET == true with input as {"STREET": "123 some street"}
}

test_column_name_starts_with_street if {
	classifier_address.output.street_name == true with input as {"street_name": "123 some street"}
}

test_insensitive_column_name_starts_with_street if {
	classifier_address.output.STREET_Name == true with input as {"STREET_Name": "123 some street"}
}

test_column_name_zip if {
	classifier_address.output.zip == true with input as {"zip": "11111"}
}

test_insensitive_column_name_zip if {
	classifier_address.output.ZIP == true with input as {"ZIP": "11111"}
}

test_column_name_zipcode if {
	classifier_address.output.zipcode == true with input as {"zipcode": "11111"}
}

test_insensitive_column_name_zipcode if {
	classifier_address.output.ZIPCODE == true with input as {"ZIPCODE": "11111"}
}
