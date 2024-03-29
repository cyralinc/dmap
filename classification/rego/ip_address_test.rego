package classifier_ip_address

import rego.v1

test_no_label if {
    output.column == false with input as {"column":"invalid"}
}

test_ipv4_google if {
    output.column == true with input as {"column":"8.8.8.8"}
}

test_ipv4_localhost if {
    output.column == true with input as {"column":"127.0.0.1"}
}

test_ipv4_internal if {
    output.column == true with input as {"column":"10.1.1.1"}
}

test_ipv6 if {
    output.column == true with input as {"column":"2001:db8:3333:4444:5555:6666:7777:8888"}
}

test_ipv6_google if {
    output.column == true with input as {"column":"2001:4860:4860::8888"}
}
