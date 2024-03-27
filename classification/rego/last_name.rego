package classifier_last_name

import rego.v1

output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := "UNLABELED"

classify(key, _) := "LAST_NAME" if {
	contains(lower(key), "last")
	contains(lower(key), "name")
}

classify(key, _) := "LAST_NAME" if {
	contains(lower(key), "sur")
	contains(lower(key), "name")
}
