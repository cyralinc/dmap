package classifier_cvv

test_cvv_key {
	output.CVV == "CVV" with input as {"CVV":"test"}
}

test_cvv_key {
	output.cvv == "CVV" with input as {"cvv":"test"}
}

test_cvv_key {
	output.CvV == "CVV" with input as {"CvV":"test"}
}

test_cvv_pattern {
	output.message == "CVV" with input as {"message":"451"}
}

test_cvv_pattern {
	output.message == "CVV" with input as {"message":"5061"}
}

test_cvv_pattern {
	output.message == "CVV" with input as {"message":"123"}
}

test_cvv_pattern {
	output.message == "UNLABELED" with input as {"message":"12345"}
}
