package classifier_full_name

import rego.v1

test_fn_pattern if {
	output.full_name == true with input as {"full_name":"John"}
}

test_fn_pattern if {
	output.full_name == true with input as {"full_name":"Robert"}
}

test_fn_pattern if {
	output.fullname == true with input as {"fullname":"Robert"}
}

test_fn_pattern if {
	output.fullName == true with input as {"fullName":"Robert"}
}

test_fn_pattern if {
	output.Full_Name == true with input as {"Full_Name":"Robert"}
}

test_fn_pattern if {
	output.FullName == true with input as {"FullName":"Robert"}
}
