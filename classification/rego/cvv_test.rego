package classifier_cvv

import rego.v1

test_cvv_key if {
	output.CVV == true with input as {"CVV":"test"}
}

test_cvv_key if {
	output.cvv == true with input as {"cvv":"test"}
}

test_cvv_key if {
	output.CvV == true with input as {"CvV":"test"}
}

test_cvv_pattern if {
	output.message == true with input as {"message":"451"}
}

test_cvv_pattern if {
	output.message == true with input as {"message":"5061"}
}

test_cvv_pattern if {
	output.message == true with input as {"message":"123"}
}

test_cvv_pattern if {
	output.message == false with input as {"message":"12345"}
}
