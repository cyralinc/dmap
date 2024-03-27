package classifier_first_name

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "FIRST_NAME" if {
	contains(lower(key), "first")
	contains(lower(key), "name")
}

classify(key, _) := "FIRST_NAME" if {
	contains(lower(key), "given")
	contains(lower(key), "name")
}
