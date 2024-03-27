package classifier_full_name

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "FULL_NAME" if {
	contains(lower(key), "full")
	contains(lower(key), "name")
}
