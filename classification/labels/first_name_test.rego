package classifier_first_name_test

import data.classifier_first_name
import rego.v1

test_fn_pattern if {
	classifier_first_name.output.first_name == true with input as {"first_name": "John"}
	classifier_first_name.output.first_name == true with input as {"first_name": "Robert"}
	classifier_first_name.output.firstname == true with input as {"firstname": "Robert"}
	classifier_first_name.output.firstName == true with input as {"firstName": "Robert"}
	classifier_first_name.output.First_Name == true with input as {"First_Name": "Robert"}
	classifier_first_name.output.FirstName == true with input as {"FirstName": "Robert"}
}

test_gn_pattern if {
	classifier_first_name.output.given_name == true with input as {"given_name": "John"}
	classifier_first_name.output.given_name == true with input as {"given_name": "Robert"}
	classifier_first_name.output.givenname == true with input as {"givenname": "Robert"}
	classifier_first_name.output.givenName == true with input as {"givenName": "Robert"}
	classifier_first_name.output.Given_Name == true with input as {"Given_Name": "Robert"}
	classifier_first_name.output.GivenName == true with input as {"GivenName": "Robert"}
}
