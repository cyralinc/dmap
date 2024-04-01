package classifier_cvv_test

import data.classifier_cvv
import rego.v1

test_cvv_key if {
	classifier_cvv.output.CVV == true with input as {"CVV": "test"}
	classifier_cvv.output.cvv == true with input as {"cvv": "test"}
	classifier_cvv.output.CvV == true with input as {"CvV": "test"}
}

test_cvv_pattern if {
	classifier_cvv.output.message == true with input as {"message": "451"}
	classifier_cvv.output.message == true with input as {"message": "5061"}
	classifier_cvv.output.message == true with input as {"message": "123"}
	classifier_cvv.output.message == false with input as {"message": "12345"}
}
