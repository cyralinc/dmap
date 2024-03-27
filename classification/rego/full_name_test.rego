package classifier_full_name

test_fn_pattern {
	output.full_name == "FULL_NAME" with input as {"full_name":"John"}
}

test_fn_pattern {
	output.full_name == "FULL_NAME" with input as {"full_name":"Robert"}
}

test_fn_pattern {
	output.fullname == "FULL_NAME" with input as {"fullname":"Robert"}
}

test_fn_pattern {
	output.fullName == "FULL_NAME" with input as {"fullName":"Robert"}
}

test_fn_pattern {
	output.Full_Name == "FULL_NAME" with input as {"Full_Name":"Robert"}
}

test_fn_pattern {
	output.FullName == "FULL_NAME" with input as {"FullName":"Robert"}
}
