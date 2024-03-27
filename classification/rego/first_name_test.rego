package classifier_first_name

test_fn_pattern {
	output.first_name == "FIRST_NAME" with input as {"first_name":"John"}
}

test_fn_pattern {
	output.first_name == "FIRST_NAME" with input as {"first_name":"Robert"}
}

test_fn_pattern {
	output.firstname == "FIRST_NAME" with input as {"firstname":"Robert"}
}

test_fn_pattern {
	output.firstName == "FIRST_NAME" with input as {"firstName":"Robert"}
}

test_fn_pattern {
	output.First_Name == "FIRST_NAME" with input as {"First_Name":"Robert"}
}

test_fn_pattern {
	output.FirstName == "FIRST_NAME" with input as {"FirstName":"Robert"}
}

test_gn_pattern {
	output.given_name == "FIRST_NAME" with input as {"given_name":"John"}
}

test_gn_pattern {
	output.given_name == "FIRST_NAME" with input as {"given_name":"Robert"}
}

test_gn_pattern {
	output.givenname == "FIRST_NAME" with input as {"givenname":"Robert"}
}

test_gn_pattern {
	output.givenName == "FIRST_NAME" with input as {"givenName":"Robert"}
}

test_gn_pattern {
	output.Given_Name == "FIRST_NAME" with input as {"Given_Name":"Robert"}
}

test_gn_pattern {
	output.GivenName == "FIRST_NAME" with input as {"GivenName":"Robert"}
}
