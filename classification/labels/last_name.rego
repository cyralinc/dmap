package classifier_last_name

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, _) if {
	contains(lower(key), "last")
	contains(lower(key), "name")
}

classify(key, _) if {
	contains(lower(key), "sur")
	contains(lower(key), "name")
}
