package classifier_address

import rego.v1

test_no_label if {
    output.column == false with input as {"column":"invalid"}
}

test_column_name_state if {
    output.state == true with input as {"state":"AZ"}
}

test_insensitive_column_name_state if {
    output.STATE == true with input as {"STATE":"AZ"}
}

test_column_name_address if {
    output.address == true with input as {"address":"123 some street"}
}

test_insensitive_column_name_address if {
    output.ADDRESS == true with input as {"ADDRESS":"123 some street"}
}

test_column_name_contains_address if {
    output.some_address_here == true with input as {"some_address_here":"123 some street"}
}

test_insensitive_column_name_contains_address if {
    output.some_ADDRESS_Here == true with input as {"some_ADDRESS_Here":"123 some street"}
}

test_column_name_street if {
    output.street == true with input as {"street":"123 some street"}
}

test_insensitive_column_name_street if {
    output.STREET == true with input as {"STREET":"123 some street"}
}

test_column_name_starts_with_street if {
    output.street_name == true with input as {"street_name":"123 some street"}
}

test_insensitive_column_name_starts_with_street if {
    output.STREET_Name == true with input as {"STREET_Name":"123 some street"}
}

test_column_name_zip if {
    output.zip == true with input as {"zip":"11111"}
}

test_insensitive_column_name_zip if {
    output.ZIP == true with input as {"ZIP":"11111"}
}

test_column_name_zipcode if {
    output.zipcode == true with input as {"zipcode":"11111"}
}

test_insensitive_column_name_zipcode if {
    output.ZIPCODE == true with input as {"ZIPCODE":"11111"}
}
