package classifier_phone_number

test_no_label {
    output.column == "UNLABELED" with input as {"column":"invalid"}
}

test_valid_us_phone {
    output.message == "PHONE" with input as {"message":"7175551212"}
}

test_valid_us_phone {
    output.message == "PHONE" with input as {"message":"17175551212"}
}
