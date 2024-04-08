package classifier_passport_test

import data.classifier_passport
import rego.v1

test_passport_key if {
	classifier_passport.output.passport == true with input as {"passport": "not-a-passport-number"}
}

# United States
test_passport_pattern_us if {
	classifier_passport.output.message == true with input as {"message": "A12345678"}
}

# Canada
test_passport_pattern_canada if {
	classifier_passport.output.message == true with input as {"message": "G12345678"}
}

# United Kingdom
test_passport_pattern_uk if {
	classifier_passport.output.message == true with input as {"message": "GH1234567"}
}

# Australia
test_passport_pattern_aus if {
	classifier_passport.output.message == true with input as {"message": "P12345678"}
}

# India
test_passport_pattern_india if {
	classifier_passport.output.message == true with input as {"message": "P1234567"}
}

# Germany
test_passport_pattern_germany if {
	classifier_passport.output.message == true with input as {"message": "E12345678"}
}

# China
test_passport_pattern_china if {
	classifier_passport.output.message == true with input as {"message": "E123456789"}
}

# Japan
test_passport_pattern_japan if {
	classifier_passport.output.message == true with input as {"message": "P1234567"}
}

# South Korea
test_passport_pattern_south_korea if {
	classifier_passport.output.message == true with input as {"message": "M12345678"}
}

# Brazil
test_passport_pattern_brazil if {
	classifier_passport.output.message == true with input as {"message": "G12345678"}
}

# Mexico
test_passport_pattern_mexico if {
	classifier_passport.output.message == true with input as {"message": "E12345678"}
}

# South Africa (diplomat, standard covered by other countries)
test_passport_pattern_sa if {
	classifier_passport.output.message == true with input as {"message": "D123456789"}
}
