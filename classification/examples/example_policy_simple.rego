package classifier

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(_, val) := "CCN" if {
	regex.match(
		`\A(4[0-9]{3}[0-9]{4}[0-9]{4}[0-9](?:[0-9]{3})?|[0-9]{4}[-][0-9]{4}[-][0-9]{4}[-][0-9]{4}|5[1-5][0-9]{2}[0-9]{4}[0-9]{4}[0-9]{4}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})\z`,
		val
	)
}

classify(key, val) := "AGE" if {
	contains(lower(key), "age")
	regex.match(`\A((\d{1,2})|1[0-1]\d)\z`, val)
}
