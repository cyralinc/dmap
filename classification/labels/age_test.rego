package classifier_age_test

import data.classifier_age
import rego.v1

test_no_label if {
	classifier_age.output.column == false with input as {"column": "invalid"}
}

test_column_name_age_invalid_age if {
	classifier_age.output.age == false with input as {"age": "120"}
}

test_insensitive_column_name_age_invalid if {
	classifier_age.output.AGE == false with input as {"AGE": "120"}
}

test_column_name_age_single_digit if {
	classifier_age.output.age == true with input as {"age": "1"}
}

test_insensitive_column_name_age_single_digit if {
	classifier_age.output.AGE == true with input as {"AGE": "1"}
}

test_column_name_age_double_digit if {
	classifier_age.output.age == true with input as {"age": "10"}
}

test_insensitive_column_name_age_double_digit if {
	classifier_age.output.AGE == true with input as {"AGE": "10"}
}

test_column_name_age_triple_digit if {
	classifier_age.output.age == true with input as {"age": "100"}
}

test_insensitive_column_name_age_triple_digit if {
	classifier_age.output.AGE == true with input as {"AGE": "100"}
}
