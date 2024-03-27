package classifier_email

test_no_label {
    output.column == "UNLABELED" with input as {"column":"invalid"}
}

test_valid_email_com {
    output.message == "EMAIL" with input as {"message":"me@me.com"}
}

test_valid_email_us {
    output.message == "EMAIL" with input as {"message":"me@state.pa.us"}
}
