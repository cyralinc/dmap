package classifier_first_name

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k)
}

default classify(_) := false

classify(key) if {
	contains(lower(key), "first")
	contains(lower(key), "name")
}

classify(key) if {
	contains(lower(key), "given")
	contains(lower(key), "name")
}
