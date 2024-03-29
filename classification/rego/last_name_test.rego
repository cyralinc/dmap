package classifier_last_name

import rego.v1

test_ln_pattern if {
	output.last_name == true with input as {"last_name":"John"}
}

test_ln_pattern if {
	output.last_name == true with input as {"last_name":"Robert"}
}

test_ln_pattern if {
	output.lastname == true with input as {"lastname":"Robert"}
}

test_ln_pattern if {
	output.lastName == true with input as {"lastName":"Robert"}
}

test_ln_pattern if {
	output.Last_Name == true with input as {"Last_Name":"Robert"}
}

test_ln_pattern if {
	output.LastName == true with input as {"LastName":"Robert"}
}

test_sn_pattern if {
	output.sur_name == true with input as {"sur_name":"John"}
}

test_sn_pattern if {
	output.sur_name == true with input as {"sur_name":"Robert"}
}

test_sn_pattern if {
	output.surname == true with input as {"surname":"Robert"}
}

test_sn_pattern if {
	output.surName == true with input as {"surName":"Robert"}
}

test_sn_pattern if {
	output.Sur_Name == true with input as {"Sur_Name":"Robert"}
}

test_sn_pattern if {
	output.SurName == true with input as {"SurName":"Robert"}
}
