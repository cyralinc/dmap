package classifier_first_name

import rego.v1

test_fn_pattern if {
	output.first_name == true with input as {"first_name":"John"}
}

test_fn_pattern if {
	output.first_name == true with input as {"first_name":"Robert"}
}

test_fn_pattern if {
	output.firstname == true with input as {"firstname":"Robert"}
}

test_fn_pattern if {
	output.firstName == true with input as {"firstName":"Robert"}
}

test_fn_pattern if {
	output.First_Name == true with input as {"First_Name":"Robert"}
}

test_fn_pattern if {
	output.FirstName == true with input as {"FirstName":"Robert"}
}

test_gn_pattern if {
	output.given_name == true with input as {"given_name":"John"}
}

test_gn_pattern if {
	output.given_name == true with input as {"given_name":"Robert"}
}

test_gn_pattern if {
	output.givenname == true with input as {"givenname":"Robert"}
}

test_gn_pattern if {
	output.givenName == true with input as {"givenName":"Robert"}
}

test_gn_pattern if {
	output.Given_Name == true with input as {"Given_Name":"Robert"}
}

test_gn_pattern if {
	output.GivenName == true with input as {"GivenName":"Robert"}
}
