package classifier_phone_number

import rego.v1

test_no_label if {
    output.column == false with input as {"column":"invalid"}
}

test_valid_us_phone if {
    output.message == true with input as {"message":"7175551212"}
}

test_valid_us_phone if {
    output.message == true with input as {"message":"17175551212"}
}
