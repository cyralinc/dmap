package classifier_age

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, val) if {
	lower(key) == "age"
	regex.match(`\A((\d{1,2})|1[0-1]\d)\z`, val)
}
