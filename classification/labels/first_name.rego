package classifier_first_name

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k, input[k])
}

default classify(_, _) := false

classify(key, _) if {
	contains(lower(key), "first")
	contains(lower(key), "name")
}

classify(key, _) if {
	contains(lower(key), "given")
	contains(lower(key), "name")
}
