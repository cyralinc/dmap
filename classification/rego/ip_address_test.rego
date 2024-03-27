package classifier_ip_address

test_no_label {
    output.column == "UNLABELED" with input as {"column":"invalid"}
}

test_ipv4_google {
    output.column == "IP_ADDRESS" with input as {"column":"8.8.8.8"}
}

test_ipv4_localhost {
    output.column == "IP_ADDRESS" with input as {"column":"127.0.0.1"}
}

test_ipv4_internal {
    output.column == "IP_ADDRESS" with input as {"column":"10.1.1.1"}
}

test_ipv6 {
    output.column == "IP_ADDRESS" with input as {"column":"2001:db8:3333:4444:5555:6666:7777:8888"}
}

test_ipv6_google {
    output.column == "IP_ADDRESS" with input as {"column":"2001:4860:4860::8888"}
}
