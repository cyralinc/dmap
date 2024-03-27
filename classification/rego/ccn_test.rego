package classifier_ccn

test_no_label {
    output.column == "UNLABELED" with input as {"column":"invalid"}
}

test_valid_amex_ccn {
    output.message == "CCN" with input as {"message":"370136066365291"}
}

test_valid_visa_ccn {
    output.message == "CCN" with input as {"message":"4613688275707134"}
}

test_valid_mastercard_ccn {
    output.message == "CCN" with input as {"message":"5423909386888564"}
}

# TODO : Fix pattern to match
test_valid_mastercard_ccn {
    output.message == "CCN" with input as {"message":"2701306282695666"}
}

test_valid_discover_ccn {
    output.message == "CCN" with input as {"message":"6536673682309236"}
}
