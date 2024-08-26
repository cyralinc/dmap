package classifier_full_name

import rego.v1

# METADATA
# entrypoint: true
output[k] := v if {
	some k in object.keys(input)
	v := classify(k)
}

default classify(_) := false

classify(key) if {
	contains(lower(key), "full")
	contains(lower(key), "name")
}
