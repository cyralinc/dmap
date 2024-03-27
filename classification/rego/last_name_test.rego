package classifier_last_name

test_ln_pattern {
	output.last_name == "LAST_NAME" with input as {"last_name":"John"}
}

test_ln_pattern {
	output.last_name == "LAST_NAME" with input as {"last_name":"Robert"}
}

test_ln_pattern {
	output.lastname == "LAST_NAME" with input as {"lastname":"Robert"}
}

test_ln_pattern {
	output.lastName == "LAST_NAME" with input as {"lastName":"Robert"}
}

test_ln_pattern {
	output.Last_Name == "LAST_NAME" with input as {"Last_Name":"Robert"}
}

test_ln_pattern {
	output.LastName == "LAST_NAME" with input as {"LastName":"Robert"}
}

test_sn_pattern {
	output.sur_name == "LAST_NAME" with input as {"sur_name":"John"}
}

test_sn_pattern {
	output.sur_name == "LAST_NAME" with input as {"sur_name":"Robert"}
}

test_sn_pattern {
	output.surname == "LAST_NAME" with input as {"surname":"Robert"}
}

test_sn_pattern {
	output.surName == "LAST_NAME" with input as {"surName":"Robert"}
}

test_sn_pattern {
	output.Sur_Name == "LAST_NAME" with input as {"Sur_Name":"Robert"}
}

test_sn_pattern {
	output.SurName == "LAST_NAME" with input as {"SurName":"Robert"}
}
