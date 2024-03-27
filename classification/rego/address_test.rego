package classifier_address

test_no_label {
    output.column == "UNLABELED" with input as {"column":"invalid"}
}

test_column_name_state {
    output.state == "ADDRESS" with input as {"state":"AZ"}
}

test_insensitive_column_name_state {
    output.STATE == "ADDRESS" with input as {"STATE":"AZ"}
}

test_column_name_address {
    output.address == "ADDRESS" with input as {"address":"123 some street"}
}

test_insensitive_column_name_address {
    output.ADDRESS == "ADDRESS" with input as {"ADDRESS":"123 some street"}
}

test_column_name_contains_address {
    output.some_address_here == "ADDRESS" with input as {"some_address_here":"123 some street"}
}

test_insensitive_column_name_contains_address {
    output.some_ADDRESS_Here == "ADDRESS" with input as {"some_ADDRESS_Here":"123 some street"}
}

test_column_name_street {
    output.street == "ADDRESS" with input as {"street":"123 some street"}
}

test_insensitive_column_name_street {
    output.STREET == "ADDRESS" with input as {"STREET":"123 some street"}
}

test_column_name_starts_with_street {
    output.street_name == "ADDRESS" with input as {"street_name":"123 some street"}
}

test_insensitive_column_name_starts_with_street {
    output.STREET_Name == "ADDRESS" with input as {"STREET_Name":"123 some street"}
}

test_column_name_zip {
    output.zip == "ADDRESS" with input as {"zip":"11111"}
}

test_insensitive_column_name_zip {
    output.ZIP == "ADDRESS" with input as {"ZIP":"11111"}
}

test_column_name_zipcode {
    output.zipcode == "ADDRESS" with input as {"zipcode":"11111"}
}

test_insensitive_column_name_zipcode {
    output.ZIPCODE == "ADDRESS" with input as {"ZIPCODE":"11111"}
}
