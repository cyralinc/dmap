package classifier_age

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, val) := "AGE" if {
	lower(key) == "age"
	regex.match(`\A((\d{1,2})|1[0-1]\d)\z`, val)
}
