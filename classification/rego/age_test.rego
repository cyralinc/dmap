package classifier_age

test_no_label {
    output.column == "UNLABELED" with input as {"column":"invalid"}
}

test_column_name_age_invalid_age {
    output.age == "UNLABELED" with input as {"age":"120"}
}

test_insensitive_column_name_age_invalid {
    output.AGE == "UNLABELED" with input as {"AGE":"120"}
}

test_column_name_age_single_digit {
    output.age == "AGE" with input as {"age":"1"}
}

test_insensitive_column_name_age_single_digit {
    output.AGE == "AGE" with input as {"AGE":"1"}
}

test_column_name_age_double_digit {
    output.age == "AGE" with input as {"age":"10"}
}

test_insensitive_column_name_age_double_digit {
    output.AGE == "AGE" with input as {"AGE":"10"}
}

test_column_name_age_triple_digit {
    output.age == "AGE" with input as {"age":"100"}
}

test_insensitive_column_name_age_triple_digit {
    output.AGE == "AGE" with input as {"AGE":"100"}
}
