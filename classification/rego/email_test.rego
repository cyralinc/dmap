package classifier_email

import rego.v1

test_no_label if {
    output.column == false with input as {"column":"invalid"}
}

test_valid_email_com if {
    output.message == true with input as {"message":"me@me.com"}
}

test_valid_email_us if {
    output.message == true with input as {"message":"me@state.pa.us"}
}
